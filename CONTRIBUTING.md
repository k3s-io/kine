# Contributing to Kine #

Thanks for taking the time to contribute to Kine!

Please review and follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Contributing is not limited to writing code and submitting a PR. Feel free to submit an [issue](https://github.com/k3s-io/kine/issues/new/choose) or comment on an existing one to report a bug, provide feedback, or suggest a new feature. You can also join the discussion on [slack](https://rancher-users.slack.com/channels/k3s).

Of course, contributing code is more than welcome! To keep things simple, if you're fixing a small issue, you can simply submit a PR and we will pick it up. However, if you're planning to submit a bigger PR to implement a new feature or fix a relatively complex bug, please open an issue that explains the change and the motivation for it. If you're addressing a bug, please explain how to reproduce it.

## AI Guidance

When using AI tools to help prepare an issue, use the applicable [issue template](.github/ISSUE_TEMPLATE)
and limit generated content to the information requested by that template.

Using AI tools to help write your PR is acceptable, but as the author, you are responsible for
understanding every change. If you used AI tools in preparing your PR, you must disclose this in the
description of your PR. Listing AI tooling as a co-author, co-signing commits using an AI tool, or using
the `assisted-by`, `co-developed` or similar commit trailer is not allowed.

Be considerate of maintainer resources. Large AI generated PRs, commit messages, and issue
reports are not allowed. Issues with excessive AI prose ("Why it matters" sections and lengthy
contrived steps to reproduce) may be ignored or rejected.

PRs may be rejected solely due to inappropriate addition of tests. Test should not be:
* Excessively trivial (ie, validates that input=output, or covers only 3rd party code)
* Replaceable by a succinct E2E test
* Unnecessarily scaffolded
* Excessively large

Do not leave the first review of AI generated changes to the reviewers. Verify the changes (code review,
testing, etc.) before submitting your PR. Reviewers may ask questions about your AI-assisted code, and if
you cannot explain why a change was made, the PR will be closed.

When responding to review comments, you must do so without relying on AI tools. Reviewers want to engage
directly with you, not with generated responses. If you do not engage directly with reviewers, the PR will
be closed.

### Reviewing, addressing feedback, and merging
Generally, pull requests need two approvals from maintainers to be merged. One exception to this is when a PR is simply a "pull through" that is just updating a dependency from other Rancher-managed vendor packages or any minor third-party vendor update. In this case, only one approval is needed.

When addressing review feedback, it is helpful to the reviewer if additional changes are made in new commits. This allows the reviewer to easily see the delta between what they previously reviewed and the changes you added to address their feedback.

Once a PR has the necessary approvals, it can be merged. Here's how the merge should be handled:
- If the PR is a single logical commit, the merger should use the "Rebase and merge" option. This keeps the git commit history very clean and simple and eliminates noise from "merge commits."
- If the PR is more than one logical commit, the merger should use the "Create a merge commit" option.
- If the PR consists of more than one commit because the author added commits to address feedback, the commits should be squashed into a single commit (or more than one logical commit, if it is a big feature that needs more commits). This can be achieved in one of two ways:
  - The merger can use the "Squash and merge" option. If they do this, the merger is responsible for cleaning up the commit message according to the previously stated commit message guidance.
  - The pull request author, after getting the requisite approvals, can reorganize the commits as they see fit (using, for example, git rebase -i) and re-push.

## Developer Certificate Of Origin ##

To contribute to this project, you must agree to the Developer Certificate of Origin (DCO) for each commit you make. The DCO is a simple statement that you, as a contributor, have the legal right to make the contribution.

See the [DCO](DCO) file for the full text of what you must agree to.

To signify that you agree to the DCO for a commit, you add a line to the git
commit message:

```txt
Signed-off-by: Jane Smith <jane.smith@example.com>
```

In most cases, you can add this signoff to your commit automatically with the
`-s` flag to `git commit`. Please use your real name and a reachable email address.
