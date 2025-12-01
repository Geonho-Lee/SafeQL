/// 질문, SQL 방언, Knowledge 등을 바탕으로
/// 주석(코멘트) 형태의 안내 문구를 만들어주는 함수
pub fn generate_comment_prompt(context: &str, query: &str) -> String {
    let base_prompt = "Using valid PostgreSQL and understanding External Knowledge";
    let knowledge_prompt = format!("External Knowledge: {context}");

    return format!(
    r#"-- {knowledge_prompt}\n
       -- {base_prompt}, answer the following questions for the tables provided above.
       -- {query}
    "#
    )
}

/// Chain of Thought(COT) 식으로 "단계별 사고 후 쿼리를 생성하라" 라는 문구를 만들어주는 함수
pub fn generate_cot_prompt() -> String {
    return "\nGenerate the SoftQL for the above question after thinking step by step: ".to_string();
}

/// 최종 응답에 들어갈 지시사항(instruction) 문구를 만드는 함수
pub fn generate_instruction_prompt() -> String {
    return r#"
    In your response, you do not need to mention your intermediate steps.
    Do not include any comments in your response.
    Do not need to start with the symbol ```
    Do not generate the SQL code.
    You only need to return the result SoftQL code.
    Be careful about the order of operators (join() -> filter() -> group() -> map() -> order())
    "#.to_string();
}

/// few-shot 예시 프롬프트 (테이블 스키마 + 예시 질문/답변) 생성
pub fn generate_few_shot_prompt() -> String {
    // singer, song 두 개의 테이블
    let schema = r#"CREATE TABLE singer
                        (
                            singer_id   TEXT NOT NULL PRIMARY KEY,
                            nation      TEXT NOT NULL,
                            sname       TEXT NULL,
                            dname       TEXT NULL,
                            cname       TEXT NULL,
                            age         INTEGER NOT NULL,
                            year        INTEGER NOT NULL,
                            birth_year  INTEGER NULL,
                            salary      REAL NULL,
                            city        TEXT NULL,
                            phone_number INTEGER NULL
                        );

                        CREATE TABLE song
                        (
                            song_id      TEXT NOT NULL PRIMARY KEY,
                            title        TEXT NOT NULL,
                            singer_id    TEXT NOT NULL,
                            release_year INTEGER NOT NULL,
                            FOREIGN KEY (singer_id) REFERENCES singer (singer_id)
                        );
                        "#;
    let softql_prompt = r#"-- External Knowledge: age = year - birth_year;
                                 -- Using valid PostgreSQL and external knowledge, answer the following question for the tables provided above.
                                 -- How many songs are sung by singers in the USA who are older than 27?
                                 Generate the SoftQL for the above question after thinking step by step."#;

    let softql_cot_result = r#"song.join(singer, equals(song.singer_id, singer.singer_id))
                                    .filter(equals(singer.nation, 'USA') AND greater(minus(singer.year, singer.birth_year), 27))
                                    .aggregate(count(song.song_id))"#;

    return format!("{schema}\n{softql_prompt}\n{softql_cot_result}");
}

/// SoftQL에 대한 설명 프롬프트(주석 형태)
pub fn generate_softql_explanation_prompt() -> String {
    r#"
    -- **SoftQL explanation (function-based DSL)**:
    SoftQL is a high-level query language that allows you to express queries in a more LLM-friendly way.

    0. Tables:
        - Each table is represented by a name (e.g., customers, yearmonth).

    1. Operators:
        - join(table, function_predicate): joins 'table' based on the given predicate
        - filter(function_predicate): filters rows based on the given predicate
        - aggregate(function_expression): aggregates rows based on the given expression
        - project(function_expression): projects/transforms rows based on the given expression
        - order(function_expression): orders rows based on the given expression

    2. Function-based Predicate:
        - A predicate is composed of one or more function calls, connected by logical operators: AND, OR, NOT.
        - You can invent any function name (e.g., equals, greater, etc.).
        - Example of a single condition (function call):
            equals(customers.customerid, yearmonth.customerid)
        - Example of multiple conditions:
            equals(person.city, 'Seoul') AND greater(person.age, 20)
        - You can also use NOT:
            equals(person.city, 'Seoul') AND NOT greater(person.age, 20)

    3. Function-based Expression:
        - An expression describes a single aggregation, transformation, or ordering rule, also written as a function call.
        - Again, there is no predefined set of function names; you can create your own.
        - Example:
            calcSum(sales.amount)
            convertDateToYear(user.birthday)
            customers.last_purchase_date
        - You can nest function calls if needed:
            round(calcAverage(ratings.score))

    5. Chaining & Syntax:
        - Use dot chaining for multiple operators (e.g., table.join().join().filter().aggregate().project().order()).
        - Operators must be in the following order: join() -> filter() -> aggregate() -> project() -> order().
        - You can use the same operator multiple times if necessary (e.g., .join().join()...).
    "#
    .to_string()
}


pub fn generate_text2softql_prompt(
    schema: &str,
    context: &str,
    query: &str,
) -> String {
    let softql_explanation_prompt = generate_softql_explanation_prompt();
    let few_shot_prompt = generate_few_shot_prompt();
    let comment_prompt = generate_comment_prompt(context, query);
    let cot_prompt = generate_cot_prompt();
    let instruction_prompt = generate_instruction_prompt();

    return format!(
        r#"{softql_explanation_prompt}
        {few_shot_prompt}
        {schema}
        {comment_prompt}
        {cot_prompt}
        {instruction_prompt}
        "#
    );
}