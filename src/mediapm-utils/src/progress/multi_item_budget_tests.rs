use super::*;
use std::sync::Arc;
use std::thread;

#[test]
fn new_creates_empty() {
    let b = MultiItemBudget::new();
    assert_eq!(b.item_count(), 0);
    assert_eq!(b.aggregate(), (0, 0));
}

#[test]
fn with_capacity_pre_allocates() {
    let b = MultiItemBudget::with_capacity(10);
    assert_eq!(b.item_count(), 0);
}

#[test]
fn add_item_increases_count() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    assert_eq!(b.item_count(), 1);
    b.add_item(200);
    assert_eq!(b.item_count(), 2);
}

#[test]
fn add_item_sets_initial_total() {
    let mut b = MultiItemBudget::new();
    b.add_item(42);
    assert_eq!(b.snap(0), (0, 42));
}

#[test]
fn item_count_reflects_adds() {
    let mut b = MultiItemBudget::new();
    assert_eq!(b.item_count(), 0);
    b.add_item(10);
    assert_eq!(b.item_count(), 1);
    b.add_item(20);
    b.add_item(30);
    assert_eq!(b.item_count(), 3);
}

#[test]
fn set_total_updates_item_total() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.set_total(0, 250);
    assert_eq!(b.snap(0), (0, 250));
}

#[test]
#[should_panic(expected = "< current pos")]
fn set_total_panics_below_position() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.advance(0, 50);
    b.set_total(0, 30);
}

#[test]
fn advance_increases_position() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.advance(0, 30);
    assert_eq!(b.snap(0), (30, 100));
    b.advance(0, 20);
    assert_eq!(b.snap(0), (50, 100));
}

#[test]
fn advance_multiple_items_independently() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.add_item(200);
    b.advance(0, 10);
    b.advance(1, 20);
    assert_eq!(b.snap(0), (10, 100));
    assert_eq!(b.snap(1), (20, 200));
}

#[test]
#[should_panic(expected = "would exceed total")]
fn advance_panics_on_overflow() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.advance(0, 101);
}

#[test]
fn set_pos_works() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.set_pos(0, 50);
    assert_eq!(b.snap(0), (50, 100));
}

#[test]
#[should_panic(expected = "> total")]
fn set_pos_panics_on_exceed_total() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.set_pos(0, 101);
}

#[test]
#[should_panic(expected = "< current")]
fn set_pos_panics_on_decrease() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.set_pos(0, 50);
    b.set_pos(0, 30);
}

#[test]
fn snap_returns_item_state() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    assert_eq!(b.snap(0), (0, 100));
    b.advance(0, 42);
    assert_eq!(b.snap(0), (42, 100));
}

#[test]
#[should_panic(expected = "index out of bounds")]
fn snap_panics_on_bad_index() {
    let b = MultiItemBudget::new();
    let _ = b.snap(0);
}

#[test]
fn aggregate_sums_all_items() {
    let mut b = MultiItemBudget::new();
    b.add_item(100);
    b.add_item(200);
    b.add_item(300);
    b.advance(0, 10);
    b.advance(1, 20);
    b.advance(2, 30);
    assert_eq!(b.aggregate(), (60, 600));
}

#[test]
fn aggregate_indeterminate_items_contribute_zero() {
    let mut b = MultiItemBudget::new();
    b.add_item(0); // indeterminate
    b.add_item(100);
    b.advance(1, 50);
    assert_eq!(b.aggregate(), (50, 100));
}

#[test]
fn default_is_empty() {
    let b = MultiItemBudget::default();
    assert_eq!(b.item_count(), 0);
    assert_eq!(b.aggregate(), (0, 0));
}

#[test]
fn concurrent_read_write_no_data_races() {
    // Pre-populate then wrap in Arc
    let mut inner = MultiItemBudget::new();
    inner.add_item(1000);
    inner.add_item(1000);
    let b = Arc::new(inner);
    let b_clone = Arc::clone(&b);
    let writer = thread::spawn(move || {
        for _ in 0..100 {
            b_clone.advance(0, 5);
        }
    });
    let b_clone2 = Arc::clone(&b);
    let reader = thread::spawn(move || {
        for _ in 0..100 {
            let (_pos, total) = b_clone2.snap(1);
            assert!(total == 1000 || total == 1500);
        }
    });
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(b.snap(0), (500, 1000));
}

#[test]
fn send_sync_trait_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<MultiItemBudget>();
    assert_sync::<MultiItemBudget>();
}
