#![cfg(not(debug_assertions))]

use crate::key_hint;
use crate::legacy_core::config::Config;
use crate::render::Insets;
use crate::render::renderable::ColumnRenderable;
use crate::render::renderable::Renderable;
use crate::render::renderable::RenderableExt as _;
use crate::selection_list::selection_option_row;
use crate::tui::FrameRequester;
use crate::tui::Tui;
use crate::tui::TuiEvent;
use crate::updates;
use color_eyre::Result;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::Widget;
use ratatui::style::Stylize as _;
use ratatui::text::Line;
use ratatui::widgets::Clear;
use ratatui::widgets::WidgetRef;
use tokio_stream::StreamExt;

const RELEASE_NOTES_URL: &str = "https://github.com/openai/codex/releases/latest";

pub(crate) async fn run_update_prompt_if_needed(tui: &mut Tui, config: &Config) -> Result<()> {
    let Some(latest_version) = updates::get_upgrade_version_for_popup(config) else {
        return Ok(());
    };

    let mut screen = UpdatePromptScreen::new(tui.frame_requester(), latest_version.clone());
    tui.draw(u16::MAX, |frame| {
        frame.render_widget_ref(&screen, frame.area());
    })?;

    let events = tui.event_stream();
    tokio::pin!(events);

    while !screen.is_done() {
        if let Some(event) = events.next().await {
            tui.screen_size_for_event(&event)?;
            match event {
                TuiEvent::Key(key_event) => screen.handle_key(key_event),
                TuiEvent::Paste(_) => {}
                TuiEvent::Draw | TuiEvent::Resume | TuiEvent::Resize(_) => {
                    tui.draw(u16::MAX, |frame| {
                        frame.render_widget_ref(&screen, frame.area());
                    })?;
                }
            }
        } else {
            break;
        }
    }

    match screen.selection() {
        Some(UpdateSelection::NotNow) | None => Ok(()),
        Some(UpdateSelection::DontRemind) => {
            if let Err(err) = updates::dismiss_version(config, screen.latest_version()).await {
                tracing::error!("Failed to persist update dismissal: {err}");
            }
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpdateSelection {
    NotNow,
    DontRemind,
}

struct UpdatePromptScreen {
    request_frame: FrameRequester,
    latest_version: String,
    current_version: String,
    highlighted: UpdateSelection,
    selection: Option<UpdateSelection>,
}

impl UpdatePromptScreen {
    fn new(request_frame: FrameRequester, latest_version: String) -> Self {
        Self {
            request_frame,
            latest_version,
            current_version: env!("CARGO_PKG_VERSION").to_string(),
            highlighted: UpdateSelection::NotNow,
            selection: None,
        }
    }

    fn handle_key(&mut self, key_event: KeyEvent) {
        if key_event.kind == KeyEventKind::Release {
            return;
        }
        if key_event.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key_event.code, KeyCode::Char('c') | KeyCode::Char('d'))
        {
            self.select(UpdateSelection::NotNow);
            return;
        }
        match key_event.code {
            KeyCode::Up | KeyCode::Char('k') => self.set_highlight(self.highlighted.prev()),
            KeyCode::Down | KeyCode::Char('j') => self.set_highlight(self.highlighted.next()),
            KeyCode::Char('1') => self.select(UpdateSelection::NotNow),
            KeyCode::Char('2') => self.select(UpdateSelection::DontRemind),
            KeyCode::Enter => self.select(self.highlighted),
            KeyCode::Esc => self.select(UpdateSelection::NotNow),
            _ => {}
        }
    }

    fn set_highlight(&mut self, highlight: UpdateSelection) {
        if self.highlighted != highlight {
            self.highlighted = highlight;
            self.request_frame.schedule_frame();
        }
    }

    fn select(&mut self, selection: UpdateSelection) {
        self.highlighted = selection;
        self.selection = Some(selection);
        self.request_frame.schedule_frame();
    }

    fn is_done(&self) -> bool {
        self.selection.is_some()
    }

    fn selection(&self) -> Option<UpdateSelection> {
        self.selection
    }

    fn latest_version(&self) -> &str {
        self.latest_version.as_str()
    }
}

impl UpdateSelection {
    fn next(self) -> Self {
        match self {
            UpdateSelection::NotNow => UpdateSelection::DontRemind,
            UpdateSelection::DontRemind => UpdateSelection::NotNow,
        }
    }

    fn prev(self) -> Self {
        match self {
            UpdateSelection::NotNow => UpdateSelection::DontRemind,
            UpdateSelection::DontRemind => UpdateSelection::NotNow,
        }
    }
}

impl WidgetRef for &UpdatePromptScreen {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        Clear.render(area, buf);
        let mut column = ColumnRenderable::new();

        column.push("");
        column.push(Line::from(vec![
            "  ✨\u{200A}".bold().cyan(),
            "A newer Codex release is available.".bold(),
            " ".into(),
            format!(
                "{current} -> {latest}",
                current = self.current_version,
                latest = self.latest_version
            )
            .dim(),
        ]));
        column.push("");
        column.push(
            Line::from(vec![
                "Release notes: ".dim(),
                RELEASE_NOTES_URL.dim().underlined(),
            ])
            .inset(Insets::tlbr(0, 2, 0, 0)),
        );
        column.push("");
        column.push(selection_option_row(
            0,
            "Continue without updating".to_string(),
            self.highlighted == UpdateSelection::NotNow,
        ));
        column.push(selection_option_row(
            1,
            "Hide this notice for this version".to_string(),
            self.highlighted == UpdateSelection::DontRemind,
        ));
        column.push("");
        column.push(
            Line::from(vec![
                "Press ".dim(),
                key_hint::plain(KeyCode::Enter).into(),
                " to continue".dim(),
            ])
            .inset(Insets::tlbr(0, 2, 0, 0)),
        );
        column.render(area, buf);
        crate::terminal_hyperlinks::mark_underlined_hyperlink(buf, area, RELEASE_NOTES_URL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_backend::VT100Backend;
    use crate::tui::FrameRequester;
    use crossterm::event::KeyCode;
    use crossterm::event::KeyEvent;
    use crossterm::event::KeyModifiers;
    use ratatui::Terminal;
    use ratatui::widgets::FrameExt;

    fn new_prompt() -> UpdatePromptScreen {
        UpdatePromptScreen::new(FrameRequester::test_dummy(), "9.9.9".into())
    }

    #[test]
    fn update_prompt_snapshot() {
        let screen = new_prompt();
        let mut terminal = Terminal::new(VT100Backend::new(80, 12)).expect("terminal");
        terminal
            .draw(|frame| frame.render_widget_ref(&screen, frame.area()))
            .expect("render update prompt");
        let rendered = terminal
            .backend()
            .to_string()
            .lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n");
        insta::assert_snapshot!("update_prompt_modal", rendered);
    }

    #[test]
    fn managed_update_prompt_confirm_continues_without_update() {
        let mut screen = new_prompt();
        screen.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(screen.is_done());
        assert_eq!(screen.selection(), Some(UpdateSelection::NotNow));
    }

    #[test]
    fn update_prompt_dismiss_option_hides_notice_for_current_version() {
        let mut screen = new_prompt();
        screen.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        screen.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(screen.is_done());
        assert_eq!(screen.selection(), Some(UpdateSelection::DontRemind));
    }

    #[test]
    fn update_prompt_ctrl_c_skips_update() {
        let mut screen = new_prompt();
        screen.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(screen.is_done());
        assert_eq!(screen.selection(), Some(UpdateSelection::NotNow));
    }

    #[test]
    fn update_prompt_navigation_wraps_between_entries() {
        let mut screen = new_prompt();
        screen.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(screen.highlighted, UpdateSelection::DontRemind);
        screen.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(screen.highlighted, UpdateSelection::NotNow);
    }
}
