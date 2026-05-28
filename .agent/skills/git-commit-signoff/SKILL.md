---
name: git-commit-signoff
description: Use whenever creating, amending, rebasing, squashing, fixing up, or modifying Git commits in this repository. Ensures every commit created or changed by the AI includes a Signed-off-by trailer using Git's built-in signoff mechanism and a Co-Authored-By trailer identifying the AI tool.
---
# Git Commit Sign-off Skill

This repository requires Git commits created or modified by the AI to include:

1. a `Signed-off-by` trailer; and
2. a `Co-Authored-By` trailer identifying the AI tool that created or modified the commit.

Always use Git's built-in sign-off mechanism for `Signed-off-by` instead of manually typing the sign-off trailer.

## Core rule

When creating a new commit, always use one of:

```bash
git commit -s
```

```bash
git commit --signoff
```

When committing with an inline message, use:

```bash
git commit -s -m "commit message"
```

Do not create commits with plain `git commit` unless the commit message already contains a valid `Signed-off-by` trailer and the user explicitly asked not to amend it.

Every commit created or modified by the AI must also include an AI co-author trailer, for example:

```text
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

Use the actual AI tool name and email identity configured for this repository or requested by the user. Do not invent an AI identity.

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

The AI co-author trailer makes it clear that an AI tool helped create or modify the commit.

## AI co-author identity

Before creating or modifying commits, determine which AI co-author trailer should be used.

If the repository, user, or existing contribution policy specifies an AI co-author identity, use that exact identity.

Examples:

```text
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

```text
Co-Authored-By: ChatGPT <noreply@openai.com>
```

If no AI co-author identity is configured or provided, ask the user which AI co-author trailer to use before creating the commit.

Do not guess the AI tool name, version, or email address.

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

## Creating a signed-off commit with AI co-author trailer

Use:

```bash
git commit -s
```

or with a message:

```bash
git commit -s -m "component: short description"
```

For multi-line commit messages, include the AI co-author trailer as a commit message paragraph:

```bash
git commit -s \
  -m "component: short description" \
  -m "Longer explanation of the change." \
  -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

If using an editor, let `git commit -s` add the `Signed-off-by` trailer, then add the AI co-author trailer to the trailer block.

## Amending the latest commit

When amending the latest commit, always preserve or add sign-off and preserve or add the AI co-author trailer.

If the message may be edited:

```bash
git commit --amend --signoff
```

If the message should stay unchanged and already contains the correct AI co-author trailer:

```bash
git commit --amend --signoff --no-edit
```

If the latest commit is missing the AI co-author trailer, amend the message and add it:

```bash
git commit --amend --signoff
```

If the latest commit already has a `Signed-off-by` trailer, `--signoff` may add another trailer depending on the current identity and message state. Check the final message after amending.

## Fixing a commit that missed sign-off or AI co-author

If the latest commit was accidentally created without sign-off, run:

```bash
git commit --amend --signoff --no-edit
```

If the latest commit was accidentally created without the AI co-author trailer, run:

```bash
git commit --amend
```

Then add the correct trailer, for example:

```text
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

Finally verify:

```bash
git show -s --format=full
```

## Verifying trailers

After creating or modifying a commit, verify the latest commit contains both required trailers:

```bash
git show -s --format=full
```

or:

```bash
git log -1 --pretty=full
```

Look for lines like:

```text
Signed-off-by: Name <email@example.com>
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

## Rebasing, squashing, fixing up, or rewording

When modifying existing commits through rebase, ensure every resulting commit still has sign-off and AI co-author trailers.

For an interactive rebase:

```bash
git rebase -i HEAD~N
```

If a commit needs sign-off, mark it as `edit`, then run:

```bash
git commit --amend --signoff --no-edit
git rebase --continue
```

If a commit needs the AI co-author trailer, mark it as `reword` or `edit`, then add the correct trailer to the commit message.

Repeat until all edited commits have the required trailers.

If squashing commits, ensure the final squashed commit message contains both:

```text
Signed-off-by: Name <email@example.com>
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

Prefer finishing the squash, then running:

```bash
git commit --amend --signoff
```

and adding the AI co-author trailer if it is missing.

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

The AI co-author trailer may be typed manually because Git does not generate it automatically.

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
3. Add the required AI co-author trailer.
4. Verify the latest commit has `Signed-off-by`.
5. Verify the latest commit has the correct `Co-Authored-By` trailer for the AI tool.
6. If history was rewritten and the user asked to push, use `git push --force-with-lease`.
7. Do not remove existing sign-off or co-author trailers unless explicitly requested.
