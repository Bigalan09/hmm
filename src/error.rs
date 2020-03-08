use std::{io, fmt, error};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Csv(csv::Error),
    ChronoParse(chrono::format::ParseError),
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::Csv(ref err) => err.description(),
            Error::ChronoParse(ref err) => err.description(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::Csv(ref err) => err.fmt(f),
            Error::ChronoParse(ref err) => err.fmt(f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        Error::Csv(err)
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(err: chrono::format::ParseError) -> Error {
        Error::ChronoParse(err)
    }
}