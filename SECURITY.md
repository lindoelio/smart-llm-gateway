# Security Policy

## Supported versions

Security fixes are applied to the latest version on the default branch. Before
opening a report, please confirm that the issue reproduces against the latest
available source.

## Reporting a vulnerability

Please do not disclose a suspected vulnerability in a public issue, discussion,
pull request, log, or screenshot.

Use GitHub's private vulnerability reporting for this repository when it is
available. Include a clear description, affected version or commit, minimal
reproduction steps, impact assessment, and any suggested mitigation.

If private reporting is unavailable, contact the repository owner through the
GitHub profile and share only enough information to establish a secure channel.
Do not send access material, private endpoints, customer data, prompt content,
or unredacted logs.

## What to expect

Reports are acknowledged as soon as practical. Maintainers will validate the
report, determine affected versions and impact, and coordinate a fix before
public disclosure. Please allow reasonable time for investigation and a
release; do not publish details before a coordinated disclosure date is agreed.

## Security boundaries

Operators are responsible for protecting deployment configuration, databases,
networks, and logs. Runtime configuration and request content must not be
committed to source control or shared in public support channels.

## Scope

Examples of useful reports include authentication bypasses, credential exposure,
request or response data leakage, authorization defects, unsafe routing or
fallback behavior, injection vulnerabilities, denial of service, and dependency
vulnerabilities with a demonstrable impact on this project.

Reports must be made in good faith. Do not access data that does not belong to
you, disrupt services, or perform destructive testing.
