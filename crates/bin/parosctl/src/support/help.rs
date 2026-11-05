//! The help as people read it: the doc comments the commands are built
//! from cite the issues and the design sections they follow (`#399`,
//! `#245 (Biscuit tokens)`, `§3.5`), for the developers. [`for_people`]
//! takes those references out of every `--help` text.

use clap::Command;

/// `command` and every subcommand, with no issue or section reference in
/// any about or argument help.
#[must_use]
pub fn for_people(command: Command) -> Command {
    let about = command.get_about().map(|s| strip_refs(&s.to_string()));
    let long_about = command.get_long_about().map(|s| strip_refs(&s.to_string()));
    let mut command = command.mut_args(|arg| {
        let help = arg.get_help().map(|s| strip_refs(&s.to_string()));
        let long_help = arg.get_long_help().map(|s| strip_refs(&s.to_string()));
        let arg = match help {
            Some(help) => arg.help(help),
            None => arg,
        };
        match long_help {
            Some(long_help) => arg.long_help(long_help),
            None => arg,
        }
    });
    if let Some(about) = about {
        command = command.about(about);
    }
    if let Some(long_about) = long_about {
        command = command.long_about(long_about);
    }
    command.mut_subcommands(for_people)
}

/// `text` with no `#NNN` reference (and the short name in parentheses
/// after one) and no `§N.N` section, the punctuation they leave tidied.
#[must_use]
pub fn strip_refs(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let reference = (chars[i] == '#' || chars[i] == '§')
            && chars.get(i + 1).is_some_and(char::is_ascii_digit);
        if !reference {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
            i += 1;
        }
        // A trailing dot ends the sentence, not the section.
        if chars[i - 1] == '.' {
            i -= 1;
        }
        // The short name after an issue: ` (Biscuit tokens)`.
        if chars.get(i) == Some(&' ')
            && chars.get(i + 1) == Some(&'(')
            && let Some(close) = chars[i + 2..].iter().position(|c| *c == ')')
            && !chars[i + 2..i + 2 + close].contains(&'(')
        {
            i += close + 3;
        }
    }
    tidy(&out)
}

/// The punctuation a removed reference leaves: `(, , )`, `, )`, ` ()`.
fn tidy(text: &str) -> String {
    let mut text = text.to_string();
    loop {
        let before = text.clone();
        for (from, to) in [
            (", ,", ","),
            (",,", ","),
            ("(, ", "("),
            ("(,", "("),
            (", )", ")"),
            (",)", ")"),
            (", :", ":"),
            ("( ", "("),
            (" ()", ""),
            ("()", ""),
            (" )", ")"),
            ("  ", " "),
            (" ,", ","),
            (" :", ":"),
            (" ;", ";"),
            (" .", "."),
        ] {
            text = text.replace(from, to);
        }
        if text == before {
            return text.trim().to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::{Arg, Command};

    use super::{for_people, strip_refs};

    #[test]
    fn every_about_and_help_of_the_tree_loses_its_references() {
        let tree = Command::new("parosctl")
            .about("The CLI (#220)")
            .long_about("The CLI over the client (#220, §3.5).")
            .arg(
                Arg::new("servers")
                    .long("servers")
                    .help("The servers (#257)")
                    .long_help("The servers, by name (#257 (names))"),
            )
            .arg(Arg::new("plain").long("plain"))
            .subcommand(
                Command::new("init")
                    .about("Form a cell (#277)")
                    .arg(Arg::new("members").long("members").help("Members (#216)")),
            );
        let people = for_people(tree);
        let about = |command: &Command| command.get_about().map(ToString::to_string);
        let long_about = |command: &Command| command.get_long_about().map(ToString::to_string);
        assert_eq!(about(&people).as_deref(), Some("The CLI"));
        assert_eq!(
            long_about(&people).as_deref(),
            Some("The CLI over the client.")
        );
        let help = |command: &Command, id: &str| {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .expect("the argument");
            (
                arg.get_help().map(ToString::to_string),
                arg.get_long_help().map(ToString::to_string),
            )
        };
        assert_eq!(
            help(&people, "servers"),
            (
                Some("The servers".to_string()),
                Some("The servers, by name".to_string())
            )
        );
        assert_eq!(help(&people, "plain"), (None, None), "no help stays none");
        let init = people.find_subcommand("init").expect("init");
        assert_eq!(about(init).as_deref(), Some("Form a cell"));
        assert_eq!(long_about(init), None);
        assert_eq!(help(init, "members").0.as_deref(), Some("Members"));
    }

    #[test]
    fn the_help_cites_no_issue_and_no_section() {
        assert_eq!(
            strip_refs("Cells (#216, #399): `cell list`"),
            "Cells: `cell list`"
        );
        assert_eq!(
            strip_refs("The universe entry (#245 (Biscuit tokens)): keys"),
            "The universe entry: keys"
        );
        assert_eq!(
            strip_refs("(`PAROS_ADVERTISE`, #257: a machine refuses)"),
            "(`PAROS_ADVERTISE`: a machine refuses)"
        );
        assert_eq!(
            strip_refs("decree (#196, #216, #277); then"),
            "decree; then"
        );
        assert_eq!(
            strip_refs("(the network is the trust boundary, §3.5), so"),
            "(the network is the trust boundary), so"
        );
        assert_eq!(
            strip_refs("Form an idle cell only (`cell init`, #232 (multiple cells)): no"),
            "Form an idle cell only (`cell init`): no"
        );
        assert_eq!(strip_refs("dialed (#257) [env"), "dialed [env");
        assert_eq!(strip_refs("whole (#575)"), "whole");
        assert_eq!(strip_refs("see §3.6."), "see.");
        assert_eq!(strip_refs("`HOST:PORT`, 1 (one)"), "`HOST:PORT`, 1 (one)");
    }
}
