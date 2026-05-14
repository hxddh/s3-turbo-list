// Integration tests for diff mode lifecycle stubs.
use s3_turbo_list::diff;

#[test]
fn test_init_diff_state_does_not_panic() {
    // verify that calling init_diff_state doesn't panic.
    diff::init_diff_state();
}

#[test]
fn test_diff_complete_notice_returns_string() {
    let notice = diff::diff_complete_notice();
    assert!(
        !notice.is_empty(),
        "diff_complete_notice should return a non-empty string"
    );
    assert!(notice.contains("diff"), "notice should mention 'diff'");
}
