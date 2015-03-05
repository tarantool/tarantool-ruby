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
        if ((digit & 128) == 0) return res;
    }
    rb_raise(rb_eValueError, "Response too short for ber");
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
unpack_field(VALUE self, VALUE data, VALUE field, VALUE i_o, VALUE realfield_o, VALUE serializers)
{
    int i = NUM2INT(i_o);
    const char *str = RSTRING_PTR(data);
    size_t len = RSTRING_LEN(data);
    size_t fieldsize = slice_ber((const uint8_t**)&str, &len);
    VALUE value;
    size_t offset = RSTRING_LEN(data) - len;

    if (fieldsize == 0) {
	return Qnil;
    }
    if (fieldsize > len) {
	rb_raise(rb_eValueError, "Response mailformed at field #%u fieldsize: %zu tail len: %zu", i, fieldsize, len);
    }

    if (field == sym_int || field == sym_integer) {
	if (fieldsize != 4) {
	    rb_raise(rb_eValueError, "Bad field size %zd for integer field #%u", fieldsize, i);
	}
	value = UINT2NUM(get_uint32(str));
    } else if (field == sym_str || field == sym_string) {
	if (*str == 0 && fieldsize > 0) {
	    value = rb_enc_str_new(str+1, fieldsize-1, rb_utf8_encoding());
	} else {
	    value = rb_enc_str_new(str, fieldsize, rb_utf8_encoding());
	}
    } else if (field == sym_int64) {
	if (fieldsize != 8) {
	    rb_raise(rb_eValueError, "Bad field size %zd for 64bit integer field #%u", fieldsize, i);
	}
	value = U642NUM(get_uint64(str));
    } else if (field == sym_bytes) {
	value = rb_enc_str_new(str, fieldsize, rb_ascii8bit_encoding());
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
	value = I642NUM((int64_t)get_uint64(str));
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
	    value = U642NUM(get_uint64(str));
	} else if (fieldsize == 2) {
	    value = UINT2NUM(get_uint16(str));
	} else {
	    rb_raise(rb_eValueError, "Bad field size %zd for integer field %d", fieldsize, i);
	}
    } else if (field == sym_auto) {
	value = rb_enc_str_new(str, fieldsize, rb_utf8_encoding());
	if (fieldsize == 2 || fieldsize == 4 || fieldsize == 8) {
	    value = rb_class_new_instance(1, &value, rb_cAutoType);
	}
    } else {
	int realfield = NUM2INT(realfield_o);
	VALUE serializer = rb_ary_entry(serializers, realfield);
	VALUE substr = rb_enc_str_new(str, fieldsize, rb_ascii8bit_encoding());
	if (!RTEST(serializer)) {
	    serializer = rb_funcall2(self, id_get_serializer, 1, &field);
	    rb_ary_store(serializers, realfield, serializer);
	}
	value = rb_funcall2(serializer, id_decode, 1, &substr);
    }
    rb_str_drop_bytes(data, offset + fieldsize);
    return value;
}

static VALUE
get_tail_no(VALUE self, VALUE array, VALUE index_o, VALUE tail_o)
{
    int size = RARRAY_LEN(array);
    int index = NUM2INT(index_o);
    int tail = NUM2INT(tail_o);
    int pos = index < size ? index : (size - tail + (index - size) % tail);
    return INT2NUM(pos);
}

void
Init_response_c()
{
    VALUE rb_mTarantool = rb_define_module("Tarantool");
    VALUE rb_mUnpackTuples = rb_define_module_under(rb_mTarantool, "UnpackTuples");
    VALUE rb_mUtil = rb_const_get(rb_mTarantool, rb_intern("Util"));
    rb_define_method(rb_mUnpackTuples, "_unpack_field", unpack_field, 5);
    rb_define_private_method(rb_mUnpackTuples, "get_tail_no", get_tail_no, 3);
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
