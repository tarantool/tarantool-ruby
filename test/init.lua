function truncate(space_no)
    box.space[0+space_no]:truncate()
end

function func1(space_no, arg1, arg2)
    return
        {type(space_no), space_no},
        {type(arg1), arg1},
        {type(arg2), arg2}
end

function func2(space_no, arg1, arg2)
    return
        {type(arg1), arg1},
        {type(arg2), arg2}
end

function func3(arg1, arg2)
    return
        {type(arg1), arg1},
        {type(arg2), arg2}
end
