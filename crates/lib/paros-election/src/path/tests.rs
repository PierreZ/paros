use super::{MAX_PATH, Path, PathError};

#[test]
fn the_root_and_its_children() {
    let root = Path::new("/").unwrap();
    assert!(root.is_root());
    assert_eq!(root.parent(), None);
    let a = root.child("a").unwrap();
    assert_eq!(a.as_str(), "/a");
    let b = a.child("b").unwrap();
    assert_eq!(b.as_str(), "/a/b");
    assert_eq!(b.parent(), Some(a.clone()));
    assert_eq!(a.parent(), Some(root));
    assert_eq!(b.name(), "b");
}

#[test]
fn bad_paths_are_refused() {
    assert_eq!(Path::new("a"), Err(PathError::NotAbsolute));
    assert_eq!(Path::new(""), Err(PathError::NotAbsolute));
    assert_eq!(Path::new("/a/"), Err(PathError::EmptyName));
    assert_eq!(Path::new("//a"), Err(PathError::EmptyName));
    assert_eq!(Path::new("/a/../b"), Err(PathError::Relative));
    assert_eq!(Path::new("/a\0"), Err(PathError::Nul));
    let long = format!("/{}", "x".repeat(MAX_PATH));
    assert_eq!(Path::new(&long), Err(PathError::TooLong));
    assert_eq!(Path::root().child("a/b"), Err(PathError::EmptyName));
}

#[test]
fn sequential_names_sort_as_positions() {
    let prefix = Path::new("/latch/n-").unwrap();
    let low = prefix.sequential(9).unwrap();
    let high = prefix.sequential(10).unwrap();
    assert_eq!(low.as_str(), "/latch/n-00000000000000000009");
    assert!(low < high);
    assert_eq!(low.parent(), Some(Path::new("/latch").unwrap()));
}
