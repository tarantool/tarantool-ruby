#include <ruby.h>
#include <ruby/encoding.h>

#if HAVE_STDINT_H
#include "stdint.h"
#elif defined(_MSC_VER)
typedef __int8 int8_t;
typedef unsigned __int8 uint8_t;
typedef __int16 int16_t;
typedef unsigned __int16 uint16_t;
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#else
#ifndef __int8_t_defined
typedef char int8_t;
typedef short int16_t;
typedef int int32_t;
#endif
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned int uint32_t;
#if SIZEOF_LONG==8
typedef long int64_t;
typedef unsigned long uint64_t;
#else
typedef long long int64_t;
typedef unsigned long long uint64_t;
#endif
#endif

#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline))
#elif defined(_MSC_VER)
#define FORCE_INLINE  __forceinline
#else
#define FORCE_INLINE
#endif

#if defined(_MSC_VER)
#define LL(x) (x)
#define LLU(x) (x)
#else
#define LL(x) (x##LL)
#define LLU(x) (x##LLU)
#endif

#if SIZEOF_LONG == 8
#define I642NUM(v) LONG2NUM(v)
#define U642NUM(v) ULONG2NUM(v)
#define NUM2I64(v) NUM2LONG(v)
#define NUM2U64(v) NUM2ULONG(v)
#else
#define I642NUM(v) LL2NUM(v)
#define U642NUM(v) ULL2NUM(v)
#define NUM2I64(v) NUM2LL(v)
#define NUM2U64(v) NUM2ULL(v)
#endif

#ifndef HAVE_RB_STR_DROP_BYTES
/* rubinius has no rb_str_drop_bytes */
ID aslice;
static VALUE
rb_str_drop_bytes(VALUE str, long bytes)
{
    VALUE args[2] = {0, INT2FIX(bytes)};
    rb_funcall2(str, aslice, 2, args);
    return str;
}
#endif

ID id_rshft, id_band, id_get_serializer, id_decode;
VALUE sym_int, sym_integer, sym_string, sym_str, sym_int64, sym_bytes, sym_int16;
VALUE sym_int8, sym_sint, sym_sint64, sym_sint16, sym_sint8, sym_varint, sym_auto;
VALUE rb_eValueError, rb_cAutoType;

#ifndef RARRAY_CONST_PTR
#define RARRAY_CONST_PTR(v) RARRAY_PTR(v)
#endif

static inline size_t
slice_ber(const uint8_t **str, size_t *len)
{
    size_t res = 0;
    while (*len > 0) {
        uint8_t digit = **str;
        res = (res << 7) | (digit & 127);
        (*str)++; (*len)--;
        if (digit < 128) return res;
    }
    rb_raise(rb_eValueError, "Response too short");
    return 0;
}

static inline uint16_t
get_uint16(const char *str)
{
        uint8_t *u = (uint8_t*)str;
        return *u | (*(u+1) << 8);
}
static inline uint32_t
get_uint32(const char *str)
{
        uint8_t *u = (uint8_t*)str;
        return *u | (*(u+1) << 8) | (*(u+2) << 16) | (*(u+3) << 24);
}
static inline uint64_t
get_uint64(const char *str)
{
        return (uint64_t)get_uint32(str) | ((uint64_t)get_uint32(str+4) << 32);
}

static VALUE
unpack_tuples(VALUE self, VALUE data, VALUE fields, VALUE _tail, VALUE tuples_affected)
{
    uint32_t fieldsn = RARRAY_LEN(fields);
    uint32_t tuplesn = NUM2UINT(tuples_affected);
    uint32_t tail = NUM2UINT(_tail);
    const char *str = StringValuePtr(data);
    size_t len = RSTRING_LEN(data);
    rb_encoding *utf8 = rb_utf8_encoding(), *binary = rb_ascii8bit_encoding();

    VALUE tuples = rb_ary_new2(tuplesn);
    VALUE serializers = rb_ary_new2(fieldsn);

    for (;tuplesn > 0; tuplesn--) {
        uint32_t tuplen, i, realfield;
        const char *end;
        VALUE tuple;
        if (len < 8) {
            rb_raise(rb_eValueError, "Response too short");
        }
        end = str + 8 + *(uint32_t*)str;
        tuplen = *(uint32_t*)(str+4);
        tuple = rb_ary_new2(fieldsn);
        str += 8;
        len -= 8;
        for(i = 0; i < tuplen; i++) {
            size_t fieldsize = slice_ber((const uint8_t**)&str, &len);
            VALUE field, value;
            if (fieldsize == 0) {
                rb_ary_push(tuple, Qnil);
                continue;
            }
            if (fieldsize > len) {
                rb_raise(rb_eValueError, "Response mailformed at field #%u fieldsize: %zu tail len: %zu", i, fieldsize, len);
            }
            realfield = i;
            if (i >= fieldsn) {
                if (tail == 1) {
                    realfield = fieldsn - 1;
                } else {
                    realfield = fieldsn + (i - fieldsn) % tail - tail;
                }
            }
            field = RARRAY_CONST_PTR(fields)[realfield];

            if (field == sym_int || field == sym_integer) {
                if (fieldsize != 4) {
                    rb_raise(rb_eValueError, "Bad field size %zd for integer field #%u", fieldsize, i);
                }
                value = UINT2NUM(get_uint32(str));
            } else if (field == sym_str || field == sym_string) {
                if (*str == 0 && fieldsize > 0) {
                    str++; len--; fieldsize--;
                }
                value = rb_enc_str_new(str, fieldsize, utf8);
            } else if (field == sym_int64) {
                if (fieldsize != 8) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 64bit integer field #%u", fieldsize, i);
                }
#if SIZEOF_LONG == 8
                value = ULONG2NUM(get_uint64(str));
#elif HAVE_LONG_LONG
                value = ULL2NUM(get_uint64(str));
#else
#error "Should have long long or sizeof(long) == 8"
#endif
            } else if (field == sym_bytes) {
                if (*str == 0 && fieldsize > 0) {
                    str++; len--; fieldsize--;
                }
                value = rb_enc_str_new(str, fieldsize, binary);
            } else if (field == sym_int16) {
                if (fieldsize != 2) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 16bit integer field #%u", fieldsize, i);
                }
                value = UINT2NUM(get_uint16(str));
            } else if (field == sym_int8) {
                if (fieldsize != 1) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 8bit integer field #%u", fieldsize, i);
                }
                value = UINT2NUM(*(uint8_t*)str);
            } else if (field == sym_sint) {
                if (fieldsize != 4) {
                    rb_raise(rb_eValueError, "Bad field size %zd for integer field #%u", fieldsize, i);
                }
                value = INT2NUM((int32_t)get_uint32(str));
            } else if (field == sym_sint64) {
                if (fieldsize != 8) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 64bit integer field #%u", fieldsize, i);
                }
#if SIZEOF_LONG == 8
                value = LONG2NUM((int64_t)get_uint64(str));
#elif HAVE_LONG_LONG
                value = LL2NUM((int64_t)get_uint64(str));
#endif
            } else if (field == sym_sint16) {
                if (fieldsize != 2) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 16bit integer field #%u", fieldsize, i);
                }
                value = INT2NUM((int16_t)get_uint16(str));
            } else if (field == sym_sint8) {
                if (fieldsize != 1) {
                    rb_raise(rb_eValueError, "Bad field size %zd for 8bit integer field #%u", fieldsize, i);
                }
                value = INT2NUM(*(int8_t*)str);
            } else if (field == sym_varint) {
                if (fieldsize == 4) {
                    value = UINT2NUM(get_uint32(str));
                } else if (fieldsize == 8) {
#if SIZEOF_LONG == 8
                    value = ULONG2NUM(get_uint64(str));
#elif HAVE_LONG_LONG
                    value = ULL2NUM(get_uint64(str));
#endif
                } else if (fieldsize == 2) {
                    value = UINT2NUM(get_uint16(str));
                } else {
                    rb_raise(rb_eValueError, "Bad field size %zd for integer field %d", fieldsize, i);
                }
            } else if (field == sym_auto) {
                value = rb_enc_str_new(str, fieldsize, utf8);
                if (fieldsize == 2 || fieldsize == 4 || fieldsize == 8) {
                    value = rb_class_new_instance(1, &value, rb_cAutoType);
                }
            } else {
                VALUE serializer = rb_ary_entry(serializers, realfield);
                VALUE substr = rb_enc_str_new(str, fieldsize, binary);
                if (!RTEST(serializer)) {
                    serializer = rb_funcall2(self, id_get_serializer, 1, &field);
                    rb_ary_store(serializers, realfield, serializer);
                }
                value = rb_funcall2(serializer, id_decode, 1, &substr);
            }
            str += fieldsize;
            len -= fieldsize;
            rb_ary_push(tuple, value);
        }
        if (end != str) {
            rb_raise(rb_eValueError, "Response mailformed");
        }
        rb_ary_push(tuples, tuple);
    }

    RB_GC_GUARD(data);
    return tuples;
}

void
Init_response_c()
{
    VALUE rb_mTarantool = rb_define_module("Tarantool");
    VALUE rb_mUnpackTuples = rb_define_module_under(rb_mTarantool, "UnpackTuples");
    VALUE rb_mUtil = rb_const_get(rb_mTarantool, rb_intern("Util"));
    rb_define_method(rb_mUnpackTuples, "_unpack_tuples", unpack_tuples, 4);
    rb_eValueError = rb_const_get(rb_mTarantool, rb_intern("ValueError"));
    rb_cAutoType = rb_const_get(rb_mUtil, rb_intern("AutoType"));

    sym_int = ID2SYM(rb_intern("int"));
    sym_integer = ID2SYM(rb_intern("integer"));
    sym_str = ID2SYM(rb_intern("str"));
    sym_string = ID2SYM(rb_intern("string"));
    sym_int16 = ID2SYM(rb_intern("int16"));
    sym_int64 = ID2SYM(rb_intern("int64"));
    sym_bytes = ID2SYM(rb_intern("bytes"));
    sym_int8 = ID2SYM(rb_intern("int8"));
    sym_sint = ID2SYM(rb_intern("sint"));
    sym_sint8 = ID2SYM(rb_intern("sint8"));
    sym_sint16 = ID2SYM(rb_intern("sint16"));
    sym_sint64 = ID2SYM(rb_intern("sint64"));
    sym_varint = ID2SYM(rb_intern("varint"));
    sym_auto = ID2SYM(rb_intern("auto"));
    id_get_serializer = rb_intern("get_serializer");
    id_decode = rb_intern("decode");
}
