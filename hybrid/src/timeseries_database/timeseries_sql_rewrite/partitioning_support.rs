use super::Name;
use super::TimeSeriesTable;
use log::debug;
use polars_core::export::chrono::Datelike;
use sea_query::{BinOper, ColumnRef, SimpleExpr, Value};
use std::rc::Rc;

impl TimeSeriesTable {
    pub fn add_partitioned_timestamp_conditions(&self, se: SimpleExpr) -> SimpleExpr {
        match se {
            SimpleExpr::Unary(op, inner) => SimpleExpr::Unary(
                op.clone(),
                Box::new(self.add_partitioned_timestamp_conditions(*inner)),
            ),
            SimpleExpr::FunctionCall(func, inner) => {
                let added = inner
                    .into_iter()
                    .map(|x| self.add_partitioned_timestamp_conditions(x))
                    .collect();
                SimpleExpr::FunctionCall(func.clone(), added)
            }
            SimpleExpr::Binary(left, op, right) => {
                self.rewrite_binary_expression(*left, op, *right)
            }
            _ => se,
        }
    }

    fn rewrite_binary_expression(
        &self,
        left: SimpleExpr,
        op: BinOper,
        right: SimpleExpr,
    ) -> SimpleExpr {
        let original = SimpleExpr::Binary(Box::new(left.clone()), op, Box::new(right.clone()));
        match op {
            BinOper::In => {
                debug!("Binary in expression partition rewriting not supported yet")
            }
            BinOper::NotIn => {
                debug!("Binary not_in expression partition rewriting not supported yet")
            }
            BinOper::Equal => {
                if let Some(e) = self.oper_or_original(&original, &left, &right, BinOper::NotEqual)
                {
                    return e;
                }
            }
            BinOper::NotEqual => {
                if let Some(e) = self.oper_or_original(&original, &left, &right, BinOper::NotEqual)
                {
                    return e;
                }
            }
            BinOper::SmallerThan => {
                if let Some(e) = self.smaller_than_or_original(&original, &left, &right) {
                    return e;
                }
            }
            BinOper::GreaterThan => {
                if let Some(e) = self.greater_than_or_original(&original, &left, &right) {
                    return e;
                }
            }
            BinOper::SmallerThanOrEqual => {
                if let Some(e) = self.smaller_than_or_original(&original, &left, &right) {
                    return e;
                }
            }
            BinOper::GreaterThanOrEqual => {
                if let Some(e) = self.greater_than_or_original(&original, &left, &right) {
                    return e;
                }
            }
            _ => {}
        };
        SimpleExpr::Binary(
            Box::new(self.add_partitioned_timestamp_conditions(left)),
            op,
            Box::new(self.add_partitioned_timestamp_conditions(right)),
        )
    }

    fn greater_than_or_original(
        &self,
        original: &SimpleExpr,
        left: &SimpleExpr,
        right: &SimpleExpr,
    ) -> Option<SimpleExpr> {
        if let SimpleExpr::Column(left_column) = left {
            if let Some(colname) = find_colname(left_column) {
                if &self.timestamp_column == &colname {
                    if let SimpleExpr::Value(right_value) = right {
                        if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                            let right_year = right_dt.year();
                            let right_month = right_dt.month();
                            let right_day = right_dt.day();
                            let year_greater_than_year_expr =
                                self.col_year_oper_const_year(right_year, BinOper::GreaterThan);
                            let year_equal_and_month_greater_expr = SimpleExpr::Binary(
                                Box::new(self.col_year_oper_const_year(right_year, BinOper::Equal)),
                                BinOper::And,
                                Box::new(
                                    self.col_month_oper_const_month(
                                        right_month,
                                        BinOper::GreaterThan,
                                    ),
                                ),
                            );
                            let year_equal_and_month_equal_and_day_greater = SimpleExpr::Binary(
                                Box::new(self.year_equal_and_month_equal(right_year, right_month)),
                                BinOper::And,
                                Box::new(
                                    self.col_day_oper_const_day(right_day, BinOper::GreaterThan),
                                ),
                            );
                            return Some(iterated_binoper(
                                vec![
                                    year_greater_than_year_expr,
                                    year_equal_and_month_greater_expr,
                                    year_equal_and_month_equal_and_day_greater,
                                    original.clone(),
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            } else if let SimpleExpr::Value(left_value) = &left {
                if let Value::ChronoDateTime(Some(left_dt)) = left_value {
                    if let SimpleExpr::Column(right_column) = &right {
                        if let Some(colname) = find_colname(right_column) {
                            if self.timestamp_column == colname {
                                let left_year = left_dt.year();
                                let left_month = left_dt.month();
                                let left_day = left_dt.day();
                                let year_greater_than_year_expr =
                                    self.const_year_oper_col_year(left_year, BinOper::GreaterThan);
                                let year_equal_and_month_greater_expr = SimpleExpr::Binary(
                                    Box::new(
                                        self.const_year_oper_col_year(left_year, BinOper::Equal),
                                    ),
                                    BinOper::And,
                                    Box::new(self.const_month_oper_col_month(
                                        left_month,
                                        BinOper::GreaterThan,
                                    )),
                                );
                                let year_equal_and_month_equal_and_day_greater = SimpleExpr::Binary(
                                    Box::new(
                                        self.year_equal_and_month_equal(left_year, left_month),
                                    ),
                                    BinOper::And,
                                    Box::new(
                                        self.col_day_oper_const_day(left_day, BinOper::GreaterThan),
                                    ),
                                );
                                return Some(iterated_binoper(
                                    vec![
                                        year_greater_than_year_expr,
                                        year_equal_and_month_greater_expr,
                                        year_equal_and_month_equal_and_day_greater,
                                        original.clone(),
                                    ],
                                    BinOper::Or,
                                ));
                            }
                        }
                    }
                }
            }
        }
        None
    }

    //Used for equal/non equal
    fn oper_or_original(
        &self,
        original: &SimpleExpr,
        left: &SimpleExpr,
        right: &SimpleExpr,
        oper: BinOper,
    ) -> Option<SimpleExpr> {
        if let SimpleExpr::Column(left_column) = left {
            if let Some(colname) = find_colname(left_column) {
                if &self.timestamp_column == &colname {
                    if let SimpleExpr::Value(right_value) = right {
                        if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                            let right_year = right_dt.year();
                            let right_month = right_dt.month();
                            let right_day = right_dt.day();

                            let year_not_equal =
                                self.col_year_oper_const_year(right_year, oper.clone());
                            let month_not_equal =
                                self.col_month_oper_const_month(right_month, oper.clone());
                            let day_not_equal =
                                self.col_day_oper_const_day(right_day, oper.clone());
                            return Some(iterated_binoper(
                                vec![
                                    year_not_equal,
                                    month_not_equal,
                                    day_not_equal,
                                    original.clone(),
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            }
        } else if let SimpleExpr::Value(left_value) = left {
            if let Value::ChronoDateTime(Some(left_dt)) = left_value {
                if let SimpleExpr::Column(right_column) = right {
                    if let Some(colname) = find_colname(right_column) {
                        if &self.timestamp_column == &colname {
                            let left_year = left_dt.year();
                            let left_month = left_dt.month();
                            let left_day = left_dt.day();

                            let year_not_equal =
                                self.const_year_oper_col_year(left_year, oper.clone());
                            let month_not_equal =
                                self.const_month_oper_col_month(left_month, oper.clone());
                            let day_not_equal = self.const_day_oper_col_day(left_day, oper.clone());
                            return Some(iterated_binoper(
                                vec![
                                    year_not_equal,
                                    month_not_equal,
                                    day_not_equal,
                                    original.clone(),
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            }
        }
        None
    }

    fn smaller_than_or_original(
        &self,
        original: &SimpleExpr,
        left: &SimpleExpr,
        right: &SimpleExpr,
    ) -> Option<SimpleExpr> {
        if let SimpleExpr::Column(left_column) = left {
            if let Some(colname) = find_colname(left_column) {
                if &self.timestamp_column == &colname {
                    if let SimpleExpr::Value(right_value) = right {
                        if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                            let right_year = right_dt.year();
                            let right_month = right_dt.month();
                            let right_day = right_dt.day();
                            let year_smaller_than_year_expr =
                                self.col_year_oper_const_year(right_year, BinOper::SmallerThan);
                            let year_equal_and_month_smaller_expr = SimpleExpr::Binary(
                                Box::new(self.col_year_oper_const_year(right_year, BinOper::Equal)),
                                BinOper::And,
                                Box::new(
                                    self.col_month_oper_const_month(
                                        right_month,
                                        BinOper::SmallerThan,
                                    ),
                                ),
                            );
                            let year_equal_and_month_equal_and_day_smaller = SimpleExpr::Binary(
                                Box::new(self.year_equal_and_month_equal(right_year, right_month)),
                                BinOper::And,
                                Box::new(
                                    self.col_day_oper_const_day(right_day, BinOper::SmallerThan),
                                ),
                            );
                            let year_equal_and_month_equal_and_day_equal_and_original = SimpleExpr::Binary(
                                Box::new(self.year_equal_and_month_equal_and_day_equal(right_year, right_month, right_day)),
                                BinOper::And,
                                Box::new(
                                    original.clone(),
                                ),
                            );
                            return Some(iterated_binoper(
                                vec![
                                    year_smaller_than_year_expr,
                                    year_equal_and_month_smaller_expr,
                                    year_equal_and_month_equal_and_day_smaller,
                                    year_equal_and_month_equal_and_day_equal_and_original,
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            } else if let SimpleExpr::Value(left_value) = left {
                if let Value::ChronoDateTime(Some(left_dt)) = left_value {
                    if let SimpleExpr::Column(right_column) = right {
                        if let Some(colname) = find_colname(right_column) {
                            if self.timestamp_column == colname {
                                let left_year = left_dt.year();
                                let left_month = left_dt.month();
                                let left_day = left_dt.day();
                                let year_smaller_than_year_expr =
                                    self.const_year_oper_col_year(left_year, BinOper::SmallerThan);
                                let year_equal_and_month_smaller_expr = SimpleExpr::Binary(
                                    Box::new(
                                        self.const_year_oper_col_year(left_year, BinOper::Equal),
                                    ),
                                    BinOper::And,
                                    Box::new(self.const_month_oper_col_month(
                                        left_month,
                                        BinOper::SmallerThan,
                                    )),
                                );
                                let year_equal_and_month_equal_and_day_smaller = SimpleExpr::Binary(
                                    Box::new(
                                        self.year_equal_and_month_equal(left_year, left_month),
                                    ),
                                    BinOper::And,
                                    Box::new(
                                        self.col_day_oper_const_day(left_day, BinOper::SmallerThan),
                                    ),
                                );
                                let year_equal_and_month_equal_and_day_equal_and_original = SimpleExpr::Binary(
                                Box::new(self.year_equal_and_month_equal_and_day_equal(left_year, left_month, left_day)),
                                BinOper::And,
                                Box::new(
                                    original.clone(),
                                ),
                            );
                                return Some(iterated_binoper(
                                    vec![
                                        year_smaller_than_year_expr,
                                        year_equal_and_month_smaller_expr,
                                        year_equal_and_month_equal_and_day_smaller,
                                        year_equal_and_month_equal_and_day_equal_and_original,
                                    ],
                                    BinOper::Or,
                                ));
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn year_column_box_simple_expression(&self) -> Box<SimpleExpr> {
        Box::new(SimpleExpr::Column(ColumnRef::Column(Rc::new(
            Name::Column(self.year_column.as_ref().unwrap().clone()),
        ))))
    }

    fn month_column_box_simple_expression(&self) -> Box<SimpleExpr> {
        Box::new(SimpleExpr::Column(ColumnRef::Column(Rc::new(
            Name::Column(self.month_column.as_ref().unwrap().clone()),
        ))))
    }

    fn day_column_box_simple_expression(&self) -> Box<SimpleExpr> {
        Box::new(SimpleExpr::Column(ColumnRef::Column(Rc::new(
            Name::Column(self.day_column.as_ref().unwrap().clone()),
        ))))
    }

    fn year_equal_and_month_equal_and_day_equal(
        &self,
        year: i32,
        month: u32,
        day: u32,
    ) -> SimpleExpr {
        SimpleExpr::Binary(
            Box::new(self.year_equal_and_month_equal(year, month)),
            BinOper::And,
            Box::new(self.col_day_oper_const_day(day, BinOper::Equal)),
        )
    }

    fn year_equal_and_month_equal(&self, year: i32, month: u32) -> SimpleExpr {
        SimpleExpr::Binary(
            Box::new(self.col_year_oper_const_year(year, BinOper::Equal)),
            BinOper::And,
            Box::new(self.col_month_oper_const_month(month, BinOper::Equal)),
        )
    }

    fn col_year_oper_const_year(&self, year: i32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            self.year_column_box_simple_expression(),
            oper,
            Box::new(SimpleExpr::Value(Value::Int(Some(year)))),
        )
    }

    fn const_year_oper_col_year(&self, year: i32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            Box::new(SimpleExpr::Value(Value::Int(Some(year)))),
            oper,
            self.year_column_box_simple_expression(),
        )
    }

    fn col_month_oper_const_month(&self, month: u32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            self.month_column_box_simple_expression(),
            oper,
            Box::new(SimpleExpr::Value(Value::Unsigned(Some(month)))),
        )
    }

    fn const_month_oper_col_month(&self, month: u32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            Box::new(SimpleExpr::Value(Value::Unsigned(Some(month)))),
            oper,
            self.month_column_box_simple_expression(),
        )
    }

    fn col_day_oper_const_day(&self, day: u32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            self.day_column_box_simple_expression(),
            oper,
            Box::new(SimpleExpr::Value(Value::Unsigned(Some(day)))),
        )
    }

    fn const_day_oper_col_day(&self, day: u32, oper: BinOper) -> SimpleExpr {
        SimpleExpr::Binary(
            Box::new(SimpleExpr::Value(Value::Unsigned(Some(day)))),
            oper,
            self.day_column_box_simple_expression(),
        )
    }
}

fn find_colname(cr: &ColumnRef) -> Option<String> {
    match cr {
        ColumnRef::Column(c) => Some(c.to_string()),
        ColumnRef::TableColumn(_, c) => Some(c.to_string()),
        ColumnRef::SchemaTableColumn(_, _, c) => Some(c.to_string()),
        _ => None,
    }
}

fn iterated_binoper(mut exprs: Vec<SimpleExpr>, oper: BinOper) -> SimpleExpr {
    let mut expr = exprs.remove(0);
    for e in exprs {
        expr = SimpleExpr::Binary(Box::new(expr), oper.clone(), Box::new(e))
    }
    expr
}
