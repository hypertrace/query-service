# AGENTS.md

Instructions for AI coding agents working in the query-service repository.

## Repository Overview

Query Service is a data querying layer that interfaces with Apache Pinot Data Store. It provides a table-like query interface for time series data from spans and events, supporting attribute selection, aggregations, and filtering (no JOINs). Queries are directly translated to PQL (Pinot Query Language).

This is part of the Hypertrace observability platform, serving as a data access layer for metrics and traces.

## Multi-Module Structure

This is a Gradle multi-module project with the following modules:

- **query-service-api** - Public API definitions (gRPC proto definitions, interfaces)
- **query-service-impl** - Core implementation (query translation, Pinot integration)
- **query-service-client** - Client library for consuming the service
- **query-service** - Main service entry point (server, configuration)
- **query-service-factory** - Factory patterns for service creation

## Build System

### Gradle (Kotlin DSL)

**Build tool**: Gradle with Kotlin DSL (`.gradle.kts` files)

**Key commands**:
```bash
# Build all modules
./gradlew build

# Run tests
./gradlew test

# Build Docker image
./gradlew dockerBuildImages

# Clean build
./gradlew clean build
```

**Configuration files**:
- `build.gradle.kts` - Root build configuration
- `settings.gradle.kts` - Multi-module configuration
- `gradle.properties` - Gradle properties
- Module-specific `build.gradle.kts` files in each module directory

## Testing

### Running Tests

```bash
# All tests
./gradlew test

# Specific module tests
./gradlew :query-service-impl:test

# Run with coverage
./gradlew test jacocoTestReport
```

### Testing Guidelines

- Write unit tests for query translation logic
- Mock external dependencies (Pinot client, network calls)
- Test error handling and edge cases (empty results, malformed queries)
- Use descriptive test names that explain the scenario

## Code Style

- Follow Java code style conventions
- Use meaningful variable names
- Keep methods focused and concise
- Document complex query translation logic
- Handle errors gracefully with appropriate logging

## Git Workflow

### Branch Naming

Use descriptive branch names:
- Feature work: `feature/<description>`
- Bug fixes: `fix/<description>`
- For Jira/ticket work: `<TICKET-ID>-<description>`

### Commit Messages

Format: `<type>: <description>`

Examples:
- `feat: Add support for nested aggregations`
- `fix: Handle null values in query translation`
- `refactor: Simplify Pinot query builder`
- `test: Add integration tests for complex queries`

### Pull Requests

- Create descriptive PR titles matching commit format
- Include test results and verification steps
- Reference related issues/tickets if applicable
- Ensure CI checks pass before requesting review

## Docker & Deployment

### Building Images

```bash
# Build Docker image locally
./gradlew dockerBuildImages
```

### Testing with Docker Compose

1. Commit changes to a test branch
2. Update the docker-compose configuration to use your test image
3. Run `docker-compose up` to test integration

### Helm Deployment

The service is deployed via Helm charts. Configuration is in the `helm/` directory.

## Common Tasks

### Adding New Query Features

1. Update proto definitions in `query-service-api` if needed
2. Implement query translation in `query-service-impl`
3. Add client methods in `query-service-client`
4. Write comprehensive tests
5. Update documentation

### Debugging Query Issues

1. Enable debug logging for query translation
2. Check Pinot query syntax and execution
3. Verify data schema matches expectations
4. Test with sample data using Pinot console

### Performance Optimization

- Profile query translation performance
- Optimize Pinot queries (use appropriate indexes)
- Monitor query execution time
- Cache frequently accessed data if applicable

## Dependencies & External Services

- **Apache Pinot** - Primary data store
- **gRPC** - Service communication protocol
- **Docker** - Containerization
- **Kubernetes/Helm** - Deployment orchestration

## Troubleshooting

### Build Issues

- Ensure Gradle wrapper is executable: `chmod +x gradlew`
- Clear Gradle cache: `./gradlew clean`
- Check Java version compatibility

### Test Failures

- Run tests with verbose output: `./gradlew test --info`
- Check for environment-specific issues
- Verify test data and mocks are properly configured

### Docker Build Issues

- Ensure Docker daemon is running
- Check Dockerfile syntax and base image availability
- Verify all required files are included in Docker context

## Guidelines for AI Agents

- **Read before editing**: Always read existing files to understand context
- **Preserve style**: Match existing code style and patterns
- **Test changes**: Run tests after making changes
- **Build verification**: Run full build before committing
- **Incremental changes**: Make small, focused changes rather than large rewrites
- **Documentation**: Update comments and docs when changing behavior
- **Error handling**: Always handle edge cases and errors
- **Logging**: Add appropriate logging for debugging

## Multi-Platform Compatibility

This repository follows the **make-agent-friendly** pattern:

- **.claude/** - Directory for Claude Code skills/agents/commands
- **AGENTS.md** - Primary documentation (this file)
- **CLAUDE.md** - Symlink to AGENTS.md for cross-platform compatibility
- Platform-specific directories (`.cursor`, `.windsurf`, `.opencode`) symlink to `.claude/`

All documentation and skills work across AI coding platforms.
