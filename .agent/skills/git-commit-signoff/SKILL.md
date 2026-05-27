---
name: git-commit-signoff
description: Use whenever creating, amending, rebasing, squashing, fixing up, or modifying Git commits in this repository. Ensures every commit created or changed by the AI includes a Signed-off-by trailer using Git's built-in signoff mechanism.
---

# Git Commit Sign-off Skill

This repository requires Git commits created or modified by the AI to include a `Signed-off-by` trailer.

Always use Git's built-in sign-off mechanism instead of manually typing the trailer.

## Core rule

When creating a new commit, always use one of:

```bash
git commit -s
````

```bash
git commit --signoff
```

When committing with an inline message, use:

```bash
git commit -s -m "commit message"
```

Do not create commits with plain `git commit` unless the commit message already contains a valid `Signed-off-by` trailer and the user explicitly asked not to amend it.

## Why this matters

`git commit -s` appends a trailer like:

```text
Signed-off-by: Name <email@example.com>
```

The identity comes from Git configuration:

```bash
git config user.name
git config user.email
```

Do not invent or manually write a sign-off identity. Let Git generate it.

## Before committing

Before creating a commit, check what will be committed:

```bash
git status --short
git diff --cached
```

If nothing is staged, either stage the intended files or ask the user what should be included.

When staging changes, prefer explicit paths:

```bash
git add path/to/file
```

Avoid broad staging like this unless the user clearly wants all changes committed:

```bash
git add .
```

## Creating a signed-off commit

Use:

```bash
git commit -s
```

or with a message:

```bash
git commit -s -m "component: short description"
```

For multi-line commit messages:

```bash
git commit -s -m "component: short description" -m "Longer explanation of the change."
```

## Amending the latest commit

When amending the latest commit, always preserve or add sign-off.

If the message may be edited:

```bash
git commit --amend --signoff
```

If the message should stay unchanged:

```bash
git commit --amend --signoff --no-edit
```

If the latest commit already has a `Signed-off-by` trailer, `--signoff` may add another trailer depending on the current identity and message state. Check the final message after amending.

## Fixing a commit that missed sign-off

If the latest commit was accidentally created without sign-off, run:

```bash
git commit --amend --signoff --no-edit
```

Then verify:

```bash
git show -s --format=full
```

## Verifying sign-off

After creating or modifying a commit, verify the latest commit contains a `Signed-off-by` trailer:

```bash
git show -s --format=full
```

or:

```bash
git log -1 --pretty=full
```

Look for a line like:

```text
Signed-off-by: Name <email@example.com>
```

## Rebasing, squashing, fixing up, or rewording

When modifying existing commits through rebase, ensure every resulting commit still has sign-off.

For an interactive rebase:

```bash
git rebase -i HEAD~N
```

If a commit needs sign-off, mark it as `edit`, then run:

```bash
git commit --amend --signoff --no-edit
git rebase --continue
```

Repeat until all edited commits have sign-off.

If squashing commits, ensure the final squashed commit message contains a valid `Signed-off-by` trailer. Prefer finishing the squash, then running:

```bash
git commit --amend --signoff --no-edit
```

## Force push safety

If a signed-off amend or rebase changes commits that were already pushed, do not use plain force push.

Use:

```bash
git push --force-with-lease
```

Only push when the user has asked to push or has clearly authorized pushing.

## Missing Git identity

If Git cannot create the sign-off because `user.name` or `user.email` is not configured, stop and report the issue.

Check with:

```bash
git config user.name
git config user.email
```

Suggest configuring them with:

```bash
git config user.name "Your Name"
git config user.email "you@example.com"
```

For global configuration:

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

Do not guess the user's identity.

## Do not manually type sign-off

Avoid this unless the user explicitly instructs it:

```text
Signed-off-by: Name <email@example.com>
```

Prefer:

```bash
git commit -s
```

or:

```bash
git commit --amend --signoff
```

## Commit message guidance

Use concise, conventional commit messages when possible.

Good examples:

```text
io: add vortex file sink test
```

```text
storage: handle spill writer cleanup
```

```text
tests: add coverage for vortex sink writes
```

Avoid vague messages:

```text
fix
```

```text
update
```

```text
changes
```

## Final checklist

Before finishing any commit-related task:

1. Confirm the intended files are staged.
2. Create or amend the commit using `-s` or `--signoff`.
3. Verify the latest commit has `Signed-off-by`.
4. If history was rewritten and the user asked to push, use `git push --force-with-lease`.
5. Do not remove existing sign-off trailers unless explicitly requested.
   EOF

````
