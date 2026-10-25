//! 分页请求/响应结构，对齐 ThingsBoard 的 PageLink / PageData。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageLink {
    pub page: i64,
    pub page_size: i64,
    pub text_search: Option<String>,
    pub sort_property: Option<String>,
    /// ASC / DESC（大小写不敏感，非法值按 DESC 处理）。
    pub sort_order: Option<String>,
}

impl PageLink {
    pub fn new(page: i64, page_size: i64) -> Self {
        Self {
            page,
            page_size,
            text_search: None,
            sort_property: None,
            sort_order: None,
        }
    }

    pub fn offset(&self) -> i64 {
        self.page.max(0).saturating_mul(self.limit())
    }

    pub fn limit(&self) -> i64 {
        self.page_size.clamp(1, 1000)
    }

    /// 排序列白名单校验：命中白名单返回该列，否则回退 created_time。
    pub fn sort_column<'a>(&self, allowed: &'a [&'a str]) -> &'a str {
        match &self.sort_property {
            Some(p) => allowed
                .iter()
                .copied()
                .find(|c| c == p)
                .unwrap_or("created_time"),
            None => "created_time",
        }
    }

    pub fn sort_order_sql(&self) -> &'static str {
        match self.sort_order.as_deref() {
            Some(o) if o.eq_ignore_ascii_case("asc") => "ASC",
            _ => "DESC",
        }
    }

    /// LIKE 子串匹配模式（小写，配合 LOWER(column)）。用户输入中的 `%`、`_`、`\`
    /// 以 PostgreSQL LIKE 的默认转义符 `\` 转义，按字面匹配。
    pub fn search_pattern(&self) -> Option<String> {
        self.text_search.as_ref().map(|text| {
            let mut pattern = String::with_capacity(text.len() + 2);
            pattern.push('%');
            for c in text.to_lowercase().chars() {
                if matches!(c, '%' | '_' | '\\') {
                    pattern.push('\\');
                }
                pattern.push(c);
            }
            pattern.push('%');
            pattern
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageData<T> {
    pub data: Vec<T>,
    pub total_pages: i64,
    pub total_elements: i64,
    pub has_next: bool,
}

impl<T> PageData<T> {
    pub fn new(data: Vec<T>, total_elements: i64, page: i64, page_size: i64) -> Self {
        let page_size = page_size.clamp(1, 1000);
        let total_pages = (total_elements + page_size - 1) / page_size;
        Self {
            data,
            total_pages,
            total_elements,
            has_next: page.max(0).saturating_add(1) < total_pages,
        }
    }
}

impl<T> Default for PageData<T> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            total_pages: 0,
            total_elements: 0,
            has_next: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_column_whitelist() {
        let mut link = PageLink::new(0, 10);
        assert_eq!(link.sort_column(&["name", "title"]), "created_time");
        link.sort_property = Some("name".into());
        assert_eq!(link.sort_column(&["name", "title"]), "name");
        link.sort_property = Some("name; DROP TABLE tenant;--".into());
        assert_eq!(link.sort_column(&["name", "title"]), "created_time");
    }

    #[test]
    fn search_pattern_escapes_like_wildcards() {
        let mut link = PageLink::new(0, 10);
        assert_eq!(link.search_pattern(), None);
        link.text_search = Some("Rate_100%".into());
        assert_eq!(link.search_pattern().unwrap(), "%rate\\_100\\%%");
        link.text_search = Some("C:\\Temp".into());
        assert_eq!(link.search_pattern().unwrap(), "%c:\\\\temp%");
    }

    #[test]
    fn page_data_flags() {
        let p = PageData::new(vec![1, 2], 5, 0, 2);
        assert_eq!(p.total_pages, 3);
        assert!(p.has_next);
        let p = PageData::new(vec![5], 5, 2, 2);
        assert!(!p.has_next);
    }

    #[test]
    fn page_size_and_offset_are_bounded() {
        let link = PageLink::new(i64::MAX, i64::MAX);
        assert_eq!(link.limit(), 1000);
        assert_eq!(link.offset(), i64::MAX.saturating_mul(1000));
        assert_eq!(PageLink::new(-1, 0).limit(), 1);
        assert_eq!(PageLink::new(-1, 0).offset(), 0);

        let page = PageData::new(Vec::<i32>::new(), 1001, 0, 5000);
        assert_eq!(page.total_pages, 2);
        assert!(page.has_next);
    }
}
