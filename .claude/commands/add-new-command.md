---
description: Add new Redis command support
argument-hint: [path-to-specification]
---

# Execute: Add new Redis command support

## Plan to Execute

Read specification file: `$ARGUMENTS`

## Execution Instructions

### 1. Preparations

- Follow instructions from `.agent/instructions.md`
- Go through the guide `specs/redis_commands_guide.md`

### 2. Read and Understand

- Read the ENTIRE specification carefully
- Go through Command Description and identify command type (string, list, set, etc.)
- Go through the Command API:
  - Identify required and optional arguments
  - Identify how to match Redis command arguments type to Python types
  - Identify return value and possible response types
- Check relevant Redis-Cli examples, if provided
- Review the Test Plan

### 3. Execute Tasks in Order

#### a. Navigate to the task
- Identify the files and action required
- Read existing related files if modifying

#### b. Implement the command
- Add new command method within matching trait object (for example: `redis/commands/core.py` for core Redis commands)
- Ensure overloading is implemented for sync and async API
- Follow Arguments definition section from `specs/redis_commands_guide.md` before defining command arguments
- Ensure arguments and response types consider bytes representation
- Ensure response RESP2 and RESP3 compatibility via `response_callbacks`

#### c. Verify as you go
- After each file change, check syntax
- Ensure imports are correct
- Verify types are properly defined
- Verify that response schema is similar for RESP2 and RESP3

### 4. Implement Testing Plan

After completing implementation tasks:

- Identify matching test file or create new one if needed
- Implement all test cases as separate test methods
- Ensure adding version constraint if specified in the specification
- Ensure tests cover edge cases

### 5. Run tests

- Run newly added test cases using `pytest` with RESP2 and RESP3 protocol specified via `--protocol` option
- Ensure that the same test cases passed with both protocols
- Get back to the Implementation stage if any test failed

### 6. Final Verification

Before completing:

- ✅ All tasks from plan completed
- ✅ All tests created and passing
- ✅ Code follows project conventions
- ✅ Documentation added/updated as needed

## Output Report

Provide summary:

### Completed Tasks
- List of all tasks completed
- Files created (with paths)
- Files modified (with paths)

### Tests Added
- Test files created
- Test cases implemented
- Test results


