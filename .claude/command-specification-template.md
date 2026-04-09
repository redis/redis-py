# $COMMAND_NAME command specification

## Supported version

Add supported Redis version here. For example: Redis >= 6.2.0

## Command description

Add a description of the command here.

## Command API

Specify an API for the command in the format that official docs uses. For example:

```
$COMMAND_NAME $key $member [NX|XX] [CH] [INCR]
```

## Redis-CLI examples

Add relevant Redis-CLI examples here.

## Test plan

Specify how do you want to test the command in terms of integration testing. For example:

- Test only with required arguments, assert that single value returned
- Test with required arguments and optional XX modifier, ensure that 1 returned
- ...
