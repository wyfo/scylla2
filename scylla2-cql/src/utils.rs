use std::{
    fmt::{Display, Write},
    io,
};

// match ($($tp:ident/$other:ident/$idx:tt),*;$len:literal)
macro_rules! tuples1 {
    ($macro:ident) => {
        $macro!(T0/U0/0; 1);
        $macro!(T0/U0/0, T1/U1/1; 2);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2; 3);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3; 4);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4; 5);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5; 6);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6; 7);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7; 8);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8; 9);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9; 10);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10; 11);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10, T11/U11/11; 12);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10, T11/U11/11, T12/U12/12; 13);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10, T11/U11/11, T12/U12/12, T13/U13/13; 14);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10, T11/U11/11, T12/U12/12, T13/U13/13, T14/U14/14; 15);
        $macro!(T0/U0/0, T1/U1/1, T2/U2/2, T3/U3/3, T4/U4/4, T5/U5/5, T6/U6/6, T7/U7/7, T8/U8/8, T9/U9/9, T10/U10/10, T11/U11/11, T12/U12/12, T13/U13/13, T14/U14/14, T15/U15/15; 16);
    };
}

macro_rules! tuples {
    ($macro:ident) => {
        $macro!(;0);
        $crate::utils::tuples1!($macro);
    };
}

pub(crate) use tuples;
pub(crate) use tuples1;

pub(crate) fn invalid_data(
    error: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub(crate) fn format_with_parens<T>(elems: impl IntoIterator<Item = T>) -> String
where
    T: Display,
{
    let mut s = String::new();
    write!(&mut s, "(").unwrap();
    let mut first_elem = true;
    for elem in elems {
        write!(&mut s, "{elem}").unwrap();
        if !first_elem {
            write!(&mut s, ", ").unwrap();
        }
        first_elem = false;
    }
    write!(&mut s, ")").unwrap();
    s
}

macro_rules! flags {
    ($($flag:path: $cond:expr),* $(,)?) => {{
        let mut flags  = ::enumflags2::BitFlags::empty();
        $(
            if $cond {
                flags |= $flag;
            }
        )*
        flags
    }};
}

pub(crate) use flags;
