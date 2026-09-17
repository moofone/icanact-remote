use icanact_remote::{ReplyDeliveryBudget, ReplyPayload};

#[test]
fn reply_payload_is_exact_sized_and_budget_rejects_zero_limits() {
    let payload = ReplyPayload::copy_from_slice(b"terminal");
    assert_eq!(payload.len(), 8);
    assert_eq!(payload.as_ref(), b"terminal");
    assert!(ReplyDeliveryBudget::new(0, 1, payload.clone()).is_err());
    assert!(ReplyDeliveryBudget::new(1, 0, payload).is_err());
}
