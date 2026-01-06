use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Null,
    Bool,
    Int,
    Float,
    String,
    Bytes,
    Timestamp,
    Date,
    Struct,
    List(Box<DataType>),
    Unknown,
}

impl DataType {
    pub fn merge(left: &DataType, right: &DataType) -> DataType {
        use DataType::*;

        if left == right {
            return left.clone();
        }
        if matches!(left, Null) {
            return right.clone();
        }
        if matches!(right, Null) {
            return left.clone();
        }
        if matches!(left, Unknown) || matches!(right, Unknown) {
            return Unknown;
        }

        match (left, right) {
            (Int, Float) | (Float, Int) => Float,
            (List(a), List(b)) => List(Box::new(DataType::merge(a, b))),
            (Struct, Struct) => Struct,
            (Bool, String)
            | (String, Bool)
            | (Int, String)
            | (String, Int)
            | (Float, String)
            | (String, Float)
            | (Bool, Int)
            | (Int, Bool)
            | (Bool, Float)
            | (Float, Bool) => String,
            _ => Unknown,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "null"),
            DataType::Bool => write!(f, "bool"),
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::String => write!(f, "string"),
            DataType::Bytes => write!(f, "bytes"),
            DataType::Timestamp => write!(f, "timestamp"),
            DataType::Date => write!(f, "date"),
            DataType::Struct => write!(f, "struct"),
            DataType::List(inner) => write!(f, "list<{}>", inner),
            DataType::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn sort_by_name(&mut self) {
        self.fields.sort_by(|a, b| a.name.cmp(&b.name));
    }
}
