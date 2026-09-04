pub fn redact_path(input: &str) -> String {
    let mut output = input.to_owned();
    let mut search_from = 0;
    while let Some(relative) = output[search_from..].find("/bot") {
        let start = search_from + relative;
        let token_start = start + 4;
        let end = output[token_start..]
            .find('/')
            .map(|n| token_start + n)
            .unwrap_or(output.len());
        if end > token_start {
            output.replace_range(token_start..end, "<redacted>");
            search_from = start + "/bot<redacted>".len();
        } else {
            break;
        }
    }
    output
}
