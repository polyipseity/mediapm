use super::*;
use std::sync::Arc;
use std::thread;

#[test]
fn new_sets_initial_state() {
    let b = ByteBudget::new(100);
    assert_eq!(b.pos(), 0);
    assert_eq!(b.total(), 100);
    assert_eq!(b.snap(), (0, 100));
}

#[test]
fn advance_increases_position() {
    let b = ByteBudget::new(100);
    b.advance(30);
    assert_eq!(b.pos(), 30);
    b.advance(20);
    assert_eq!(b.pos(), 50);
    println!("snap: {:?}", b.snap());
}

#[test]
#[should_panic(expected = "would exceed total")]
fn advance_panics_on_overflow() {
    let b = ByteBudget::new(100);
    b.advance(101);
}

#[test]
fn set_pos_works() {
    let b = ByteBudget::new(100);
    b.set_pos(50);
    assert_eq!(b.pos(), 50);
}

#[test]
#[should_panic(expected = "> total")]
fn set_pos_panics_on_exceed_total() {
    let b = ByteBudget::new(100);
    b.set_pos(101);
}

#[test]
#[should_panic(expected = "< current")]
fn set_pos_panics_on_decrease() {
    let b = ByteBudget::new(100);
    b.set_pos(50);
    b.set_pos(30);
}

#[test]
fn adjust_positive_increases_total() {
    let b = ByteBudget::new(100);
    b.adjust(50);
    assert_eq!(b.total(), 150);
}

#[test]
fn adjust_negative_decreases_total() {
    let b = ByteBudget::new(100);
    b.adjust(-30);
    assert_eq!(b.total(), 70);
}

#[test]
#[should_panic(expected = "below pos")]
fn adjust_negative_panics_below_pos() {
    let b = ByteBudget::new(40);
    b.advance(30);
    b.adjust(-50);
}

#[test]
fn reconcile_increases_total() {
    let b = ByteBudget::new(100);
    b.reconcile(50, 100);
    assert_eq!(b.total(), 150);
}

#[test]
fn reconcile_decreases_total() {
    let b = ByteBudget::new(100);
    b.reconcile(100, 50);
    assert_eq!(b.total(), 50);
}

#[test]
fn reconcile_equal_is_noop() {
    let b = ByteBudget::new(100);
    b.reconcile(50, 50);
    assert_eq!(b.total(), 100);
}

#[test]
fn concurrent_read_write_no_data_races() {
    let b = Arc::new(ByteBudget::new(1000));
    let b_clone = Arc::clone(&b);
    let writer = thread::spawn(move || {
        for _ in 0..100 {
            b_clone.advance(5);
        }
    });
    let b_clone2 = Arc::clone(&b);
    let reader = thread::spawn(move || {
        for _ in 0..100 {
            let (_pos, total) = b_clone2.snap();
            assert!(total == 1000 || total == 1500);
        }
    });
    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn send_sync_trait_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<ByteBudget>();
    assert_sync::<ByteBudget>();
}
