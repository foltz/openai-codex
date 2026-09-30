//! Parsing and resolution for owned status-line configuration entries.

use std::collections::HashMap;

use unicode_width::UnicodeWidthStr;

use super::status_line_setup::StatusLineItem;

const TEMPLATE_PREFIX: &str = "template:";
const MAX_TEMPLATE_SOURCE_BYTES: usize = 256;
const MAX_TEMPLATE_PLACEHOLDERS: usize = 16;
const MAX_TEMPLATE_DISPLAY_COLUMNS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StatusLineConfigEntry {
    BuiltIn(StatusLineItem),
    Template(StatusLineTemplate),
}

impl StatusLineConfigEntry {
    pub(crate) fn raw_config(&self) -> String {
        match self {
            Self::BuiltIn(item) => item.to_string(),
            Self::Template(template) => template.raw.clone(),
        }
    }

    pub(crate) fn built_in(&self) -> Option<StatusLineItem> {
        match self {
            Self::BuiltIn(item) => Some(*item),
            Self::Template(_) => None,
        }
    }

    pub(crate) fn is_template(&self) -> bool {
        matches!(self, Self::Template(_))
    }

    pub(crate) fn render(
        &self,
        variables: &HashMap<String, String>,
        built_in_value: impl FnOnce(StatusLineItem) -> Option<String>,
    ) -> Result<Option<RenderedStatusLineSegment>, TemplateResolutionError> {
        match self {
            Self::BuiltIn(item) => {
                Ok(built_in_value(*item).map(|text| RenderedStatusLineSegment {
                    text,
                    display_class: StatusLineDisplayClass::BuiltIn(*item),
                }))
            }
            Self::Template(template) => template.resolve(variables).map(|text| {
                text.map(|text| RenderedStatusLineSegment {
                    text,
                    display_class: StatusLineDisplayClass::Template,
                })
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatusLineTemplate {
    raw: String,
    tokens: Vec<TemplateToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TemplateToken {
    Literal(String),
    Placeholder(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusLineDisplayClass {
    BuiltIn(StatusLineItem),
    Template,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RenderedStatusLineSegment {
    pub(crate) text: String,
    pub(crate) display_class: StatusLineDisplayClass,
}

impl From<(StatusLineItem, String)> for RenderedStatusLineSegment {
    fn from((item, text): (StatusLineItem, String)) -> Self {
        Self {
            text,
            display_class: StatusLineDisplayClass::BuiltIn(item),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateResolutionError {
    MissingVariable { raw: String, key: String },
    InvalidVariable { raw: String, key: String },
    RenderedTooWide { raw: String },
}

impl TemplateResolutionError {
    pub(crate) fn warning_label(&self) -> String {
        match self {
            Self::MissingVariable { raw, key } => {
                format!("{raw:?} (missing variable {key:?})")
            }
            Self::InvalidVariable { raw, key } => {
                format!("{raw:?} (invalid variable {key:?})")
            }
            Self::RenderedTooWide { raw } => format!("{raw:?} (rendered value is too wide)"),
        }
    }
}

impl StatusLineTemplate {
    fn parse(raw: String) -> Result<Self, ()> {
        if raw.len() > MAX_TEMPLATE_SOURCE_BYTES || raw.chars().any(char::is_control) {
            return Err(());
        }
        let body = raw.strip_prefix(TEMPLATE_PREFIX).ok_or(())?;
        let mut tokens = Vec::new();
        let mut literal = String::new();
        let mut chars = body.char_indices().peekable();
        let mut placeholders = 0;

        while let Some((_, ch)) = chars.next() {
            match ch {
                '{' => {
                    if chars.peek().is_some_and(|(_, next)| *next == '{') {
                        chars.next();
                        literal.push('{');
                        continue;
                    }
                    if !literal.is_empty() {
                        tokens.push(TemplateToken::Literal(std::mem::take(&mut literal)));
                    }
                    let mut key = String::new();
                    let mut closed = false;
                    for (_, key_ch) in chars.by_ref() {
                        if key_ch == '}' {
                            closed = true;
                            break;
                        }
                        if key_ch == '{' {
                            return Err(());
                        }
                        key.push(key_ch);
                    }
                    if !closed || !valid_key(&key) {
                        return Err(());
                    }
                    placeholders += 1;
                    if placeholders > MAX_TEMPLATE_PLACEHOLDERS {
                        return Err(());
                    }
                    tokens.push(TemplateToken::Placeholder(key));
                }
                '}' => {
                    if chars.peek().is_some_and(|(_, next)| *next == '}') {
                        chars.next();
                        literal.push('}');
                    } else {
                        return Err(());
                    }
                }
                _ => literal.push(ch),
            }
        }
        if !literal.is_empty() {
            tokens.push(TemplateToken::Literal(literal));
        }
        Ok(Self { raw, tokens })
    }

    fn resolve(
        &self,
        variables: &HashMap<String, String>,
    ) -> Result<Option<String>, TemplateResolutionError> {
        let mut rendered = String::new();
        for token in &self.tokens {
            match token {
                TemplateToken::Literal(text) => rendered.push_str(text),
                TemplateToken::Placeholder(key) => {
                    let Some(value) = variables.get(key) else {
                        return Err(TemplateResolutionError::MissingVariable {
                            raw: self.raw.clone(),
                            key: key.clone(),
                        });
                    };
                    if value.chars().any(char::is_control)
                        || UnicodeWidthStr::width(value.as_str()) > MAX_TEMPLATE_DISPLAY_COLUMNS
                    {
                        return Err(TemplateResolutionError::InvalidVariable {
                            raw: self.raw.clone(),
                            key: key.clone(),
                        });
                    }
                    rendered.push_str(value);
                }
            }
        }
        if UnicodeWidthStr::width(rendered.as_str()) > MAX_TEMPLATE_DISPLAY_COLUMNS {
            return Err(TemplateResolutionError::RenderedTooWide {
                raw: self.raw.clone(),
            });
        }
        Ok(Some(rendered))
    }
}

fn valid_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    (1..=32).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

pub(crate) fn parse_status_line_entries(
    ids: impl IntoIterator<Item = String>,
) -> (Vec<StatusLineConfigEntry>, Vec<String>) {
    let mut entries = Vec::new();
    let mut invalid = Vec::new();
    let mut invalid_seen = std::collections::HashSet::new();
    for raw in ids {
        let entry = if raw.starts_with(TEMPLATE_PREFIX) {
            StatusLineTemplate::parse(raw.clone()).map(StatusLineConfigEntry::Template)
        } else {
            raw.parse::<StatusLineItem>()
                .map(StatusLineConfigEntry::BuiltIn)
                .map_err(|_| ())
        };
        match entry {
            Ok(entry) => entries.push(entry),
            Err(()) if invalid_seen.insert(raw.clone()) => invalid.push(format!("{raw:?}")),
            Err(()) => {}
        }
    }
    (entries, invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn variables(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn template(raw: &str) -> StatusLineTemplate {
        StatusLineTemplate::parse(raw.to_string()).expect("valid template")
    }

    #[test]
    fn parses_and_resolves_literals_placeholders_and_escapes() {
        let parsed = template("template:{{{left}}}|{right}");
        assert_eq!(
            parsed.resolve(&variables(&[("left", "L"), ("right", "R")])),
            Ok(Some("{L}|R".to_string()))
        );
    }

    #[test]
    fn enforces_source_placeholder_and_key_bounds() {
        assert!(StatusLineTemplate::parse(format!("template:{}", "a".repeat(248))).is_err());
        assert!(StatusLineTemplate::parse(format!("template:{}", "a".repeat(247))).is_ok());
        assert!(StatusLineTemplate::parse(format!("template:{}", "{a}".repeat(17))).is_err());
        assert!(StatusLineTemplate::parse(format!("template:{}", "{a}".repeat(16))).is_ok());
        for key in ["", "Upper", "has-hyphen", "has.dot", "_first"] {
            assert!(StatusLineTemplate::parse(format!("template:{{{key}}}")).is_err());
        }
        assert!(StatusLineTemplate::parse(format!("template:{{a{}}}", "0".repeat(31))).is_ok());
        assert!(StatusLineTemplate::parse(format!("template:{{a{}}}", "0".repeat(32))).is_err());
    }

    #[test]
    fn rejects_malformed_braces_and_controls() {
        for raw in [
            "template:{",
            "template:}",
            "template:{a",
            "template:{a{b}",
            "template:\n",
            "template:\u{1b}",
            "template:\u{85}",
        ] {
            assert!(
                StatusLineTemplate::parse(raw.to_string()).is_err(),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn omits_the_whole_segment_for_missing_invalid_or_wide_values() {
        let parsed = template("template:(k:{version}|{lane})");
        let missing = parsed.resolve(&variables(&[("version", "v1")]));
        assert!(matches!(
            missing,
            Err(TemplateResolutionError::MissingVariable { key, .. }) if key == "lane"
        ));
        let invalid = parsed.resolve(&variables(&[("version", "secret\nvalue"), ("lane", "dev")]));
        let warning = invalid.expect_err("invalid value").warning_label();
        assert!(!warning.contains("secret"));

        let wide = "界".repeat(33);
        assert!(matches!(
            template("template:{value}").resolve(&variables(&[("value", &wide)])),
            Err(TemplateResolutionError::InvalidVariable { .. })
        ));
        assert!(matches!(
            template("template:x{value}").resolve(&variables(&[("value", &"a".repeat(64))])),
            Err(TemplateResolutionError::RenderedTooWide { .. })
        ));
    }

    #[test]
    fn preserves_order_and_duplicate_templates() {
        let (entries, invalid) = parse_status_line_entries([
            "model-name".to_string(),
            "template:{lane}".to_string(),
            "template:{lane}".to_string(),
        ]);
        assert!(invalid.is_empty());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].raw_config(), "template:{lane}");
        assert_eq!(entries[2].raw_config(), "template:{lane}");
    }
}
