//Copyright 2022 Prediktor AS
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

grammar itotdsl;
options {language=Rust;}

query :
  pathExpression+ (FROM_DATE_TIME) (TO_DATE_TIME) (AGGREGATE)
  ;

conditionedPathExpression :
    expr=pathExpression
    (op=operator (conditionExpr=pathExpression | literal=LITERAL))?
    ;

operator:
  (op='=' | op='>' | op='<' | op='>=' | op='<=' | op='!=');

pathExpression :
  (startIdentifier=identifier | startGlue=GLUE)
  rel=relation
  unaryOperator=('+'|'*')?
  (endIdentifier=identifier | (endGlue=GLUE endIdentifier=identifier)?)
  ;

relation :
 ('='+ | '-'+ | '#'+ | '.'+ | ';'+ | ':'+ | '/'+ | '\\'+)
 ;

identifier :
    TYPE_NAME | NAME_STRING ;

TYPE_NAME:
    ALPHA+;

NAME_STRING:
    '"' ('%' | ALPHA | DIGIT | '-' | '_' )+ '"';

GLUE : '[' [0-9]+ ']';

//Literals for comparisons
LITERAL : REAL | INTEGER | BOOLEAN ;

AGGREGATE : 'aggregate ' DURATION ' ' AGG_OPERATOR;

fragment
DURATION:
    (INTEGER | REAL) TIME_UNIT;

fragment
TIME_UNIT:
    ALPHA;

fragment
AGG_OPERATOR:
    ALPHA;

//Adapted from: https://gist.github.com/jdegoes/5853435
//License is not defined for this gist
FROM_DATE_TIME : 'from ' FULL_DATE ' ' FULL_TIME;
TO_DATE_TIME : 'to ' FULL_DATE ' ' FULL_TIME;

fragment
TIME_OFFSET: 'Z' | TIME_NUM_OFFSET;

fragment
PARTIAL_TIME : TIME_HOUR ':' TIME_MINUTE ':' TIME_SECOND TIME_SEC_FRAC?;

fragment
FULL_DATE : DATE_FULL_YEAR '-' DATE_MONTH '-' DATE_M_DAY;

fragment
FULL_TIME : PARTIAL_TIME TIME_OFFSET;

fragment
DATE_FULL_YEAR : DIGIT DIGIT DIGIT DIGIT;

fragment
DATE_MONTH : DIGIT DIGIT;

fragment
DATE_M_DAY  : DIGIT DIGIT;

fragment
TIME_HOUR : DIGIT DIGIT;

fragment
TIME_MINUTE : DIGIT DIGIT;

fragment
TIME_SECOND: DIGIT DIGIT;

fragment
TIME_SEC_FRAC : '.' DIGIT+;

fragment
TIME_NUM_OFFSET : ('+' | '-') TIME_HOUR ':' TIME_MINUTE;

fragment
REAL : [0-9]+ '.' [0-9]+;

fragment
INTEGER : DIGIT+;

fragment
BOOLEAN : 'true' | 'false' ;

fragment
ALPHA : [A-Za-z]+;

fragment
DIGIT : [0-9];

WS  :  (
	' '|
	'\r'|
	'\t' |
	'\u000C' |
	'\n'
	) -> skip
    ;