<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Contributing to LakeSoul

This project welcomes contributors from any organization or background, provided they are
willing to follow the simple processes outlined below, as well as adhere to the 
[Code of Conduct](CODE_OF_CONDUCT.md).

## Joining the community

The community collaborates primarily through  `GitHub` and the instance messaging tool, `Slack`.
There is also a mailing list.
See how to join [here](https://github.com/lakesoul-io/LakeSoul/blob/main/community-guideline.md)

## Reporting an Issue

Please use the [issues][issues] section of the LakeSoul repository and search for a similar problem. If you don't find it, submit your bug, question, proposal or feature request.

Use tags to indicate parts of the LakeSoul that your issue relates to.
For example, in the case of bugs, please provide steps to reproduce it and tag your issue with `bug` and integration that has that bug, for example `spark` or `flink`.


## Contributing to the project

### Creating Pull Requests
Before sending a Pull Request with significant changes, please use the [issue tracker][issues] to discuss the potential improvements you want to make.

LakeSoul uses [GitHub's fork and pull model](https://help.github.com/articles/about-collaborative-development-models/)
to create a contribution.

To ensure your pull request is accepted, follow these guidelines:

* All changes should be accompanied by tests
* Do your best to have a [well-formed commit message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) for your change
* Do your best to have a [well-formed](https://frontside.com/blog/2020-7-reasons-for-good-pull-request-descriptions) pull request description for your change
* [Keep diffs small](https://kurtisnusbaum.medium.com/stacked-diffs-keeping-phabricator-diffs-small-d9964f4dcfa6) and self-contained
* If your change fixes a bug, please [link the issue](https://help.github.com/articles/closing-issues-using-keywords) in your pull request description
* Your pull request title should be of the form `[component] name`, where `[component]` is the part of LakeSoul repo that your PR changes. For example: `[Flink] add table source implementation`
* Use tags to indicate parts of the repository that your PR refers to

### Branching

* Use a _group_ at the beginning of your branch names:

  ```
  feature  Add or expand a feature
  bug      Fix a bug
  ```

  _For example_:

  ```
  feature/my-cool-new-feature
  bug/my-bug-fix
  bug/my-other-bug-fix
  ```

* Choose _short_ and _descriptive_ branch names
* Use dashes (`-`) to separate _words_ in branch names
* Use _lowercase_ in branch names

## Proposing changes

Create an issue and tag it as `proposal`.

In the description provide the following sections:
 - Purpose (Why?): What is the use case this is for. 
 - Proposed implementation (How?): Quick description of how do you propose to implement it. Are you proposing a new facet?

This can be just a couple paragraphs to start with.

Issue that could be splitted into several tasks should be tagged as `epic`.

## First-Time Contributors

If this is your first contribution to open source, you can [follow this tutorial][contributiontutorial] or check out [this video series][contributionvideos] to learn about the contribution workflow with GitHub.

Look for tickets labeled ['good first issue'][goodfirstissues] and ['help wanted'][helpwantedissues]. These are a great starting point if you want to contribute. Don't hesitate to ask questions about the issue if you are not sure about the strategy to follow.


[issues]: https://github.com/metas-soul/LakeSoul/issues
[contributiontutorial]: https://github.com/firstcontributions/first-contributions#first-contributions
[contributionvideos]: https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github
[goodfirstissues]: https://github.com/lakesoul-io/LakeSoul/labels/good%20first%20issue
[helpwantedissues]: https://github.com/lakesoul-io/LakeSoul/labels/help%20wanted

## Triggering CI runs from forks (committers)

CI runs on forks are disabled due to the possibility of access by external services via CI run. 
Once a contributor decides a PR is ready to be checked, they can use [this script](https://github.com/jklukas/git-push-fork-to-upstream-branch)
to trigger a CI run on a separate branch with the same commit ID. This will update the CI status of a PR.
