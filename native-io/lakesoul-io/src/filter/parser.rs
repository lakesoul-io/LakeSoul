use std::collections::HashMap;

use datafusion::logical_expr::{col, lit, Expr, Operator};
use datafusion::physical_expr::unicode_expressions::left;
use datafusion::scalar::ScalarValue;

pub struct Parser {
}

impl Parser {

    pub fn parse(filter_str: String, schema: &HashMap<String, String>) -> Expr {
        let (op, left, right) = Parser::parse_filter_str(filter_str);
        //println!("op: {}, left: {}, right: {}", op, left, right);
        if right == "null" {
            match op.as_str() {
                "eq" => {
                    let column = col(left.as_str());
                    column.is_null()
                }
                "noteq" => {
                    let column = col(left.as_str());
                    column.is_not_null()
                }
                _ => 
                    Expr::Wildcard
            }
        } else {
            match op.as_str() {
                "not" => {
                    let inner = Parser::parse(right, schema);
                    //print!("{:?}", inner);
                    Expr::not(inner)
                }
                "eq" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.eq(value)
                }
                "noteq" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.not_eq(value)
                }
                "or" => {
                    let left_expr = Parser::parse(left, schema);
                    let right_expr = Parser::parse(right, schema);
                    left_expr.or(right_expr)
                }
                "and" => {
                    let left_expr = Parser::parse(left, schema);
                    let right_expr = Parser::parse(right, schema);
                    left_expr.and(right_expr)
                }
                "gt" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.gt(value)
                }
                "gteq" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.gt_eq(value)
                }
                "lt" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.lt(value)
                }
                "lteq" => {
                    let column = col(left.as_str());
                    let value = Parser::parse_literal(left, right, schema);
                    column.lt_eq(value)
                }

                _ => 
                    Expr::Wildcard
            }
        }
    }

    fn parse_filter_str(filter: String) -> (String, String, String) {
        let op_offset = filter.find('(').unwrap();
        let (op, filter) = filter.split_at(op_offset);
        if !filter.ends_with(")") {
            panic!("Invalid filter string");
        }
        let filter = &filter[1..filter.len()-1];
        let mut k:i8 = 0;
        let mut left_offset:usize = 0;
        for (i, ch) in filter.chars().enumerate() {
            match ch {
                '(' => 
                    k += 1,
                ')' => 
                    k -= 1,
                ',' => 
                    if k==0 && left_offset==0 {
                        left_offset = i
                    },
                _ => {}
            }
        }
        if k != 0 {
            panic!("Invalid filter string");
        }
        let (left,right) = filter.split_at(left_offset);
        if op.eq("not") {
            (op.to_string(), left.to_string(), right[0..].to_string())
        } else {
            (op.to_string(), left.to_string(), right[2..].to_string())
        }
    }

    fn parse_literal(column: String, value:String, schema: &HashMap<String, String>) -> Expr {
        let datatype = schema.get(&column).unwrap();
        if datatype.len() > 7 && &datatype[..7] == "decimal" {
            let comma_offset = datatype.find(',').unwrap();
            let precision = datatype[8..comma_offset].parse::<u8>().unwrap();
            let scale = datatype[comma_offset+1..datatype.len()-1].parse::<u8>().unwrap();
            if precision <= 18 {
                Expr::Literal(ScalarValue::Decimal128(Some(value.parse::<i128>().unwrap()), precision, scale))
            } else {
                let binary_vec = Parser::parse_binary_array(value.as_str()).unwrap();
                

                let mut arr = [0u8;16];
                for idx in (0..binary_vec.len()) {
                    arr[idx + 16 - binary_vec.len()] = binary_vec[idx];
                }
                Expr::Literal(ScalarValue::Decimal128(Some(i128::from_be_bytes(arr)), precision, scale))
            }
        } else {
            match datatype.as_str() {
                "boolean" => Expr::Literal(ScalarValue::Boolean(Some(value.parse::<bool>().unwrap()))),
                "binary" => Expr::Literal(ScalarValue::Binary(Parser::parse_binary_array(value.as_str()))),
                "float" => Expr::Literal(ScalarValue::Float32(Some(value.parse::<f32>().unwrap()))),
                "double" => Expr::Literal(ScalarValue::Float64(Some(value.parse::<f64>().unwrap()))),
                "byte" => Expr::Literal(ScalarValue::Int8(Some(value.parse::<i8>().unwrap()))),
                "short" => Expr::Literal(ScalarValue::Int16(Some(value.parse::<i16>().unwrap()))),
                "integer" => Expr::Literal(ScalarValue::Int32(Some(value.parse::<i32>().unwrap()))),
                "long" => Expr::Literal(ScalarValue::Int64(Some(value.parse::<i64>().unwrap()))),
                "date" => Expr::Literal(ScalarValue::Date32(Some(value.parse::<i32>().unwrap()))),
                "timestamp" => Expr::Literal(ScalarValue::TimestampMicrosecond(Some(value.parse::<i64>().unwrap()), None)),
                "string" => {
                    // value will be wrapped by Binary("value")
                    let value = value.as_str()[8..value.len()-2].to_string();
                    Expr::Literal(ScalarValue::Utf8(Some(value)))
                }
                _ => Expr::Literal(ScalarValue::Utf8(Some(value)))
            }
        }

    }

    fn parse_binary_array(value: &str) -> Option<Vec<u8>>{
        let left_bracket_pos = value.find('[').unwrap_or(0);
        let right_bracket_pos = value.find(']').unwrap_or(0);
        if left_bracket_pos == 0 {
            None
        } else if left_bracket_pos + 1 == right_bracket_pos {
            Some(Vec::<u8>::new())
        } else {
            Some(value[left_bracket_pos+1..right_bracket_pos].to_string().replace(" ", "")
                .split(",")
                .collect::<Vec<&str>>()
                .iter()
                .map(|s| s.parse::<i16>().unwrap())
                .map(|s:i16| 
                    if s < 0 {
                        (s + 256) as u8
                    } else {
                        s as u8
                    }
                )
                .collect::<Vec<u8>>())
        }
    }


}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::result::Result;
    use crate::filter::Parser;

    #[test]
    fn test_filter_parser() -> Result<(), String> {
        let s = String::from("or(lt(a.b.c, 2.0), gt(a.b.c, 3.0))");
        // let parser = Parser::new();
        // Parser::parse(s);
        Ok(())
    }

    #[test]
    fn test_filter_parser_not() -> Result<(), String> {
        let s = String::from("not(eq(a.c, 2.9))");
        // Parser::parse(s);
        Ok(())
    }

    #[test]
    fn test_filter_parser_binary() -> Result<(), String> {
        let mut schema= HashMap::<String, String>::new();
        schema.insert("a".to_string(), "binary".to_string());
        let s = String::from("eq(a, Binary{0 reused bytes, null})");
        Parser::parse(s, &schema);
        let s = String::from("eq(a, Binary{0 reused bytes, []})");
        Parser::parse(s, &schema);
        let s = String::from("eq(a, Binary{3 reused bytes, [49, 50, 51]})");
        Parser::parse(s, &schema);
        Ok(())
    }
}