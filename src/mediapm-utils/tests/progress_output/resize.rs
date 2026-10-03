//! Terminal-resize tests.
//!
//! Bar geometry follows the *draw target's* width, not the injected dimension
//! source: `recompute_layout` reads the term it draws into. Changing only the
//! dimension source's column count therefore redraws a byte-identical frame,
//! and the tests that claimed a width resize changed the output were really
//! observing "nothing drawn yet" versus "first frame". The width contract is
//! asserted here the way it is actually reachable — by rendering the same bars
//! into draw targets of different widths. Height is different: it drives slot
//! allocation when the terminal is built with dynamic height.

use std::sync::Arc;

use mediapm_utils::progress::{SuffixComponents, TestDimensionSource};

use super::common::{bar_cells, drawn_width, mk_with_capacity, mk_with_dims, without_spinners};

/// Changing only the dimension source's columns redraws the same frame: the
/// frame width is fixed by the draw target, so this pins the fact that width
/// reactivity is not reachable through the dimension source alone.
///
/// The comparison normalizes the spinner, which advances on every draw whether or
/// not any tracked value changed.
#[test]
fn dimension_source_columns_do_not_change_the_frame() {
    let dims = Arc::new(TestDimensionSource::new((5, 80)));
    let (terminal, term) = mk_with_dims(5, 80, 4, &dims, None, false);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();
    let _child = screen.add_bar(7, "tool-a");

    screen.tick();
    let wide = term.contents();

    dims.set((5, 40));
    screen.tick();
    let narrow = term.contents();

    assert!(wide.contains("overall"), "precondition: a frame was drawn: {wide:?}");
    assert_eq!(
        without_spinners(&wide),
        without_spinners(&narrow),
        "the dimension source's columns must not drive the drawn width"
    );
    assert_eq!(
        &wide,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸      tool-a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "dimension_source_columns_do_not_change_the_frame"
    );
}

/// A narrow draw target gives up the bar before it gives up the label, and the
/// row stays on one line.
///
/// At 20 columns the spinner, the separators and the suffix leave so little of
/// the line that the fill the budget affords is exactly its four-cell floor,
/// and a floor-sized fill carries a fraction the count beside it already
/// states. So the row drops it, keeps `tool-a`, keeps `0/7`, and drops the
/// timing with the bar. This is the whole point of budgeting from the terminal
/// width: before, the prefix held its measured width and the row came back
/// wrapped, with `0s 0/d` on the line below where it read as a second bar.
#[test]
fn narrow_draw_target_drops_the_bar_not_the_label() {
    let (wide_terminal, wide_term) = mk_with_capacity(4, 40, 3);
    let (wide_screen, _overall) = wide_terminal.screen().with_overall("overall", 10).build();
    let _child = wide_screen.add_bar(7, "tool-a");
    wide_screen.tick();

    let (narrow_terminal, narrow_term) = mk_with_capacity(4, 20, 3);
    let (narrow_screen, _overall) = narrow_terminal.screen().with_overall("overall", 10).build();
    let _child = narrow_screen.add_bar(7, "tool-a");
    narrow_screen.tick();

    let wide = wide_term.contents();
    let narrow = narrow_term.contents();
    assert!(bar_cells(&wide) > 0, "the 40-column frame has room for a bar: {wide:?}");
    assert_eq!(
        bar_cells(&narrow),
        0,
        "both 20-column rows drop a fill that would be at its floor: {narrow:?}",
    );
    for line in narrow.lines() {
        assert!(drawn_width(line) <= 20, "a 20-column row must not wrap: {line:?}");
    }
    assert_eq!(
        &wide,
        concat!(
            "\n",
            "\n",
            "⠸      tool-a ░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░  0/10 0s 0/d"
        ),
        "narrow_draw_target_drops_the_bar_not_the_label/wide"
    );
    assert_eq!(
        &narrow,
        concat!("\n", "\n", "⠸      tool-a  0/7\n", "⠹     overall  0/10"),
        "narrow_draw_target_drops_the_bar_not_the_label/narrow"
    );
}

/// A wider draw target renders a wider bar: the number of bar cells grows with
/// the terminal width.
#[test]
fn wider_draw_target_expands_the_bar() {
    let (narrow_terminal, narrow_term) = mk_with_capacity(4, 40, 3);
    let (narrow_screen, _overall) = narrow_terminal.screen().with_overall("overall", 10).build();
    let child = narrow_screen.add_bar(10, "tool-a");
    child.advance(5);
    narrow_screen.tick();

    let (wide_terminal, wide_term) = mk_with_capacity(4, 120, 3);
    let (wide_screen, _overall) = wide_terminal.screen().with_overall("overall", 10).build();
    let child = wide_screen.add_bar(10, "tool-a");
    child.advance(5);
    wide_screen.tick();

    let narrow = narrow_term.contents();
    let wide = wide_term.contents();
    assert!(
        bar_cells(&wide) > bar_cells(&narrow),
        "a 120-column frame must spend more cells on the bar than a 40-column one: \
         {} vs {}",
        bar_cells(&wide),
        bar_cells(&narrow)
    );
    assert_eq!(
        &narrow,
        concat!(
            "\n",
            "\n",
            "⠼      tool-a ████░░░░░  5/10 0s 0/d\n",
            "⠹     overall ░░░░░░░░░  0/10 0s 0/d"
        ),
        "wider_draw_target_expands_the_bar/narrow"
    );
    assert_eq!(
        &wide,
        concat!(
            "\n",
            "\n",
            "⠼      tool-a ████████████████████████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  5/10 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "wider_draw_target_expands_the_bar/wide"
    );
}

/// A custom suffix that cannot fit a narrow draw target never reaches the
/// terminal: at 20 columns the suffix is cut to what the line has left after
/// the spinner, the separators and the fill floor, and the custom text is the
/// first thing to go, so neither it nor the rate/ETA behind it is drawn. The
/// wide frame in the same test renders the suffix in full, which is what makes
/// the absence observable.
///
/// This does not evidence truncation ordering: the narrow frame drops its bar
/// outright and drops the timing with it, so the only field left to fit is the
/// `custom` text, which is shortened from the front. The order suffix fields are
/// removed in is pinned by the inline `semantic_truncate_suffix_*` unit tests.
#[test]
fn narrow_draw_target_does_not_render_an_oversize_custom_suffix() {
    let (narrow_terminal, narrow_term) = mk_with_capacity(4, 20, 3);
    let (narrow_screen, _overall) = narrow_terminal.screen().with_overall("overall", 10).build();
    let child = narrow_screen.add_bar(10, "tool-a");
    child.set_suffix_components(SuffixComponents {
        custom: "already downloaded".into(),
        ..Default::default()
    });
    narrow_screen.tick();

    let (wide_terminal, wide_term) = mk_with_capacity(4, 120, 3);
    let (wide_screen, _overall) = wide_terminal.screen().with_overall("overall", 10).build();
    let child = wide_screen.add_bar(10, "tool-a");
    child.set_suffix_components(SuffixComponents {
        custom: "already downloaded".into(),
        ..Default::default()
    });
    wide_screen.tick();

    let narrow = narrow_term.contents();
    let wide = wide_term.contents();
    assert!(wide.contains("already downloaded"), "the wide frame keeps the suffix: {wide:?}");
    assert!(
        !narrow.contains("already downloaded"),
        "the narrow frame does not render it: {narrow:?}"
    );
    for line in narrow.lines() {
        assert!(drawn_width(line) <= 20, "a 20-column row must not wrap: {line:?}");
    }
    assert_eq!(
        &narrow,
        concat!("\n", "\n", "⠸   0/10 already do\n", "⠹   0/10"),
        "narrow_draw_target_does_not_render_an_oversize_custom_suffix/narrow"
    );
    assert_eq!(
        &wide,
        concat!(
            "\n",
            "\n",
            "⠸      tool-a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d already downloaded\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "narrow_draw_target_does_not_render_an_oversize_custom_suffix/wide"
    );
}

/// Shrinking the terminal height to one row evicts every child bar, and growing
/// it back reattaches the same bar with its progress intact.
#[test]
fn height_shrink_orphans_and_grow_reattaches() {
    let dims = Arc::new(TestDimensionSource::new((5, 80)));
    let (terminal, term) = mk_with_dims(5, 80, 4, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let child = screen.add_bar(7, "fetch");
    child.advance(3);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠼       fetch █████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "height_shrink_orphans_and_grow_reattaches/initial"
    );

    dims.set((1, 80));
    screen.tick();
    let shrunk = term.contents();
    assert_eq!(shrunk.lines().count(), 1, "one row leaves room for the overall bar only");
    assert!(!shrunk.contains("fetch"), "the child is evicted at one row: {shrunk:?}");
    assert_eq!(
        &shrunk,
        concat!("⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░", "  0/10 0s 0/d"),
        "height_shrink_orphans_and_grow_reattaches/shrunk"
    );

    dims.set((5, 80));
    screen.tick();
    let grown = term.contents();
    assert!(grown.contains("fetch"), "the child is reattached: {grown:?}");
    assert!(grown.contains("3/7"), "and keeps its progress: {grown:?}");
    assert_eq!(
        &grown,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹       fetch █████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/7 0s 0/d\n",
            "⠼     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "height_shrink_orphans_and_grow_reattaches/grown"
    );
}

/// Dynamic height grows the slot count to the terminal height, so the frame
/// gains rows as the terminal does — the reserved capacity is not the ceiling.
///
/// What this pins is the growth itself: two rows draw two lines, 24 rows draw
/// 24, and the bound bars keep their order with the overall at the bottom either
/// way. The ceiling the growth stops at is covered by the
/// `dynamic_height_clamps_at_the_slot_limit` test below.
#[test]
fn dynamic_height_grows_the_slot_count_to_the_terminal_height() {
    let dims = Arc::new(TestDimensionSource::new((2, 80)));
    let (terminal, term) = mk_with_dims(24, 80, 6, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();
    let _c1 = screen.add_bar(7, "fetch");

    screen.tick();
    let at_two = term.contents();
    assert_eq!(at_two.lines().count(), 2, "two rows draw two lines: {at_two:?}");
    let lines: Vec<&str> = at_two.lines().collect();
    let child = lines.iter().position(|line| line.contains("fetch")).expect("the child's line:");
    let overall =
        lines.iter().position(|line| line.contains("overall")).expect("the overall's line:");
    assert!(child < overall, "the overall stays at the bottom: {lines:?}");
    assert_eq!(
        &at_two,
        concat!(
            "⠸       fetch ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "dynamic_height_grows_the_slot_count_to_the_terminal_height/two_rows"
    );

    dims.set((24, 80));
    screen.tick();
    let at_twenty_four = term.contents();
    assert_eq!(at_twenty_four.lines().count(), 24, "the terminal grew, so the frame did too");
    let lines: Vec<&str> = at_twenty_four.lines().collect();
    let child = lines.iter().position(|line| line.contains("fetch")).expect("the child's line:");
    let overall =
        lines.iter().position(|line| line.contains("overall")).expect("the overall's line:");
    assert!(child < overall, "the overall stays at the bottom: {lines:?}");
    assert_eq!(
        at_twenty_four,
        concat!(
            "⠼       fetch ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "dynamic_height_grows_the_slot_count_to_the_terminal_height/twenty_four_rows"
    );
}

/// Shrinking the terminal below the reserved slot count removes the topmost
/// blank lines first, and the bound bars keep their order.
#[test]
fn height_shrink_removes_blank_slots() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (terminal, term) = mk_with_dims(7, 80, 6, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let _child = screen.add_bar(7, "fetch");
    screen.tick();
    let tall = term.contents();
    assert_eq!(tall.lines().count(), 6, "six rows render six reserved slots");
    assert_eq!(
        &tall,
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸       fetch ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "height_shrink_removes_blank_slots/tall"
    );

    dims.set((4, 80));
    screen.tick();
    let short = term.contents();
    assert_eq!(short.lines().count(), 4, "four rows drop the two topmost blanks");
    assert_eq!(short.lines().last().map(|line| line.contains("overall")), Some(true));
    assert_eq!(
        &short,
        concat!(
            "\n",
            "\n",
            "⠼       fetch ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "height_shrink_removes_blank_slots/short"
    );
}

/// Growing the terminal adds blank lines above the bound bars: the children
/// keep their relative order, the overall bar stays last, and no bar is
/// displaced sideways or duplicated.
#[test]
fn height_grow_appends_blank_slots_above_the_bars() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (terminal, term) = mk_with_dims(7, 80, 6, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let _old = screen.add_bar(7, "oldest");
    let _new = screen.add_bar(7, "newest");
    screen.tick();
    let short = term.contents();
    assert_eq!(short.lines().count(), 4, "four rows render four slots");
    assert_eq!(
        &short,
        concat!(
            "\n",
            "⠹      oldest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠸      newest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "height_grow_appends_blank_slots_above_the_bars/short"
    );

    dims.set((6, 80));
    screen.tick();
    let tall = term.contents();
    let lines: Vec<&str> = tall.lines().collect();
    assert_eq!(lines.len(), 6, "six rows render six slots");
    assert_eq!(lines.last().map(|line| line.contains("overall")), Some(true));
    let oldest = lines.iter().position(|line| line.contains("oldest")).expect("oldest line");
    let newest = lines.iter().position(|line| line.contains("newest")).expect("newest line");
    assert!(oldest < newest, "children keep their order: {lines:?}");
    assert!(
        lines[..oldest].iter().all(|line| line.is_empty()),
        "the growth lands as blanks above the bars: {lines:?}"
    );
    assert_eq!(
        &tall,
        concat!(
            "\n",
            "⠸      oldest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠼      newest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "\n",
            "\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "height_grow_appends_blank_slots_above_the_bars/tall"
    );
}

/// A shrink-then-grow cycle restores the frame's line count and bar order, so
/// a resize cannot leave the grid with permanently lost slots.
#[test]
fn height_shrink_then_grow_restores_the_frame() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (terminal, term) = mk_with_dims(7, 80, 6, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let _old = screen.add_bar(7, "oldest");
    let _new = screen.add_bar(7, "newest");
    screen.tick();
    let before = term.contents();
    assert_eq!(
        &before,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹      oldest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠸      newest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "height_shrink_then_grow_restores_the_frame/before"
    );

    dims.set((3, 80));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠸      oldest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠼      newest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "height_shrink_then_grow_restores_the_frame/shrunk"
    );

    dims.set((6, 80));
    screen.tick();
    let restored = term.contents();
    let before_lines: Vec<&str> = before.lines().collect();
    let restored_lines: Vec<&str> = restored.lines().collect();
    assert_eq!(restored_lines.len(), before_lines.len(), "the line count is restored");
    for label in ["oldest", "newest", "overall"] {
        assert!(restored.contains(label), "{label} is back after the cycle: {restored:?}");
    }
    assert_eq!(
        &restored,
        concat!(
            "⠼      oldest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠴      newest ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "\n",
            "\n",
            "\n",
            "⠼     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "height_shrink_then_grow_restores_the_frame/restored"
    );
}

/// Without an overall bar the child region is the whole grid, so a height
/// sequence is pure slot count: each shrink drops the topmost row, which evicts
/// the oldest child once the blank rows are gone.
///
/// Growth appends the new row below the surviving bars and reattaches the
/// evicted child there, so a bar that survives the cycle keeps its line and the
/// reattached one lands below it.
#[test]
fn height_sequence_without_overall() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (terminal, term) = mk_with_dims(4, 80, 4, &dims, None, true);
    let screen = terminal.screen().build();

    let _old = screen.add_bar(5, "bar 2");
    let _new = screen.add_bar(3, "bar 1");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹     bar 2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸     bar 1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "height_sequence_without_overall/tall"
    );

    dims.set((3, 80));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠸     bar 2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼     bar 1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "height_sequence_without_overall/three_rows"
    );

    dims.set((2, 80));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠼     bar 2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠴     bar 1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "height_sequence_without_overall/two_rows"
    );

    dims.set((1, 80));
    screen.tick();
    let one_row = term.contents();
    assert_eq!(
        &one_row, "⠦     bar 1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        "height_sequence_without_overall/one_row"
    );
    assert!(one_row.contains("bar 1"), "the newest child keeps the only slot: {one_row:?}");

    dims.set((3, 80));
    screen.tick();
    let restored = term.contents();
    assert_eq!(
        &restored,
        concat!(
            "⠧     bar 1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d\n",
            "\n",
            "⠹     bar 2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d",
        ),
        "height_sequence_without_overall/reattached"
    );
    assert!(restored.contains("bar 2"), "the evicted child is reattached: {restored:?}");
    assert!(restored.contains("bar 1"), "the surviving child kept its line: {restored:?}");
}

/// A slot limit bounds the grid: a terminal far taller than the renderer's
/// `MAX_SLOTS` ceiling grows the frame to exactly that ceiling, not to the
/// reported height.
///
/// The assertion is two-sided on purpose: a frame that stayed at the reserved
/// capacity fails it just as a frame that reached the terminal height does, so
/// it cannot pass without height adaptation clamping exactly at the limit.
#[test]
fn dynamic_height_clamps_at_the_slot_limit() {
    let dims = Arc::new(TestDimensionSource::new((1000, 80)));
    let (terminal, term) = mk_with_dims(300, 40, 4, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();
    let _child = screen.add_bar(7, "child");

    screen.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 256, "1000 reported rows must clamp to the renderer's 256-slot limit");
    assert_eq!(
        lines.last().map(|line| line.contains("overall")),
        Some(true),
        "the overall bar keeps the bottom slot at the limit: {contents:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("child")),
        "the bound child is still drawn at the limit: {contents:?}"
    );
}

/// A partial shrink to three rows keeps both active children and the overall
/// bar: the renderer evicts the blank slots, not the bars.
#[test]
fn partial_height_shrink_keeps_active_children() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (terminal, term) = mk_with_dims(7, 80, 6, &dims, None, true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let old = screen.add_bar(7, "oldest");
    let new = screen.add_bar(7, "newest");
    old.advance(2);
    new.advance(4);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸      oldest ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2/7 0s 0/d\n",
            "⠼      newest ████████████████████████████░░░░░░░░░░░░░░░░░░░░░  4/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "partial_height_shrink_keeps_active_children/tall"
    );

    dims.set((3, 80));
    screen.tick();
    let short = term.contents();
    assert_eq!(short.lines().count(), 3, "three rows render three slots");
    for label in ["oldest", "newest", "overall"] {
        assert!(
            short.contains(label),
            "a partial shrink must not evict a bound bar ({label}): {short:?}"
        );
    }
    assert_eq!(
        &short,
        concat!(
            "⠼      oldest ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2/7 0s 0/d\n",
            "⠴      newest ████████████████████████████░░░░░░░░░░░░░░░░░░░░░  4/7 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "partial_height_shrink_keeps_active_children/short"
    );
}
