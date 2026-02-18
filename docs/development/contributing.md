# Contributing to Carade

We welcome contributions to Carade! Whether you're fixing a bug, improving documentation, or adding a new feature, your help is appreciated.

## Getting Started

1.  **Fork** the repository on GitHub.
2.  **Clone** your fork locally.
3.  **Branch** off from `main` for your changes.

```bash
git checkout -b feature/my-new-feature
```

## Development Environment

### Prerequisites

*   **Java JDK 21+**
*   **Maven 3.8+**
*   **Git**

### Build

Carade is built using Maven.

```bash
mvn clean package
```

This will compile the code, run tests, and create a JAR file in the `target/` directory.

## Code Style

Carade follows standard Java coding conventions. We use **Checkstyle** to enforce code quality.

To run checkstyle manually:

```bash
mvn checkstyle:check
```

Please ensure your code passes checkstyle before submitting a Pull Request.

## Submitting Changes

1.  **Commit** your changes with clear messages.
2.  **Push** to your fork.
3.  **Open a Pull Request** (PR) against the `main` branch.
4.  **Wait** for review. We may ask for changes or clarifications.

## Adding New Commands

To add a new Redis command:

1.  Create a new class implementing `core.commands.Command` in `src/main/java/core/commands/`.
2.  Implement the `execute(ClientHandler client, List<byte[]> args)` method.
3.  Register the command in `core.commands.CommandRegistry`.
4.  Add a unit test in `src/test/java/core/commands/`.
