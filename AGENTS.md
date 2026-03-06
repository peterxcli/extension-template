# AI Agent Guidelines for DuckDB Extensions

Welcome to the DuckDB Extension repository! This `AGENTS.md` file serves as the system instruction set for any AI coding agents (Cursor, Copilot, etc.) operating in this codebase. It documents essential commands, structural context, and code style requirements.

## 1. Project Context & Structure
- **Type**: DuckDB C++ Extension
- **Core Dep**: DuckDB is included as a submodule in the `duckdb/` directory.
- **Build System**: CMake, orchestrated via `Makefile` which includes `extension-ci-tools/makefiles/duckdb_extension.Makefile`.
- **Primary Source**: Extension logic lives in `src/` and `src/include/`.
- **Tests**: SQLLogicTests (`.test` files) live in `test/sql/`.

## 2. Build & Setup Commands

Always build the project to verify compilation before submitting code changes.

- **Debug Build (Recommended for development)**:
  ```bash
  make debug
  ```
  *Binaries go to `build/debug/`*

- **Release Build (Optimized)**:
  ```bash
  make release
  ```
  *Binaries go to `build/release/`*

- **Clean the Build**:
  ```bash
  make clean
  ```

## 3. Testing Workflow

DuckDB extensions rely heavily on `SQLLogicTest`. Tests are `.test` scripts executed by the compiled `unittest` binary.

- **Run all tests (Release build)**:
  ```bash
  make test_release
  # or simply: make test
  ```

- **Run all tests (Debug build)**:
  ```bash
  make test_debug
  ```

### **Running a Single Test (Crucial for Agents)**
To run a specific test, do not use the `make test` targets. Instead, invoke the test runner executable directly and pass the path to the `.test` file or glob pattern:

- **Single test (Release)**:
  ```bash
  ./build/release/test/unittest "test/sql/my_specific_test.test"
  ```

- **Single test (Debug)**:
  ```bash
  ./build/debug/test/unittest "test/sql/my_specific_test.test"
  ```

- **Run a specific directory of tests**:
  ```bash
  ./build/debug/test/unittest "test/sql/my_feature/*"
  ```

*Agent Note:* Always use the debug executable when investigating a crash, as it provides assertions and useful stack traces.

## 4. Linting and Formatting

The codebase enforces strict `clang-format` and `clang-tidy` rules inherited from DuckDB core.

- **Format Code (Fix in-place)**:
  ```bash
  make format
  # or make format-fix
  ```
- **Check Format (Fails if unformatted)**:
  ```bash
  make format-check
  ```

*Agent Note:* Run `make format` autonomously after editing any `.cpp` or `.hpp` file before concluding a task or committing.

## 5. Code Style & Architecture Guidelines (C++)

When writing or modifying C++ code for this DuckDB extension, adhere to these established conventions:

### Namespaces and Includes
- **Namespace**: Wrap all extension logic inside `namespace duckdb { ... }`. Do not pollute the global namespace.
- **Includes**: Use standard DuckDB core headers for database operations.
  - Example: `#include "duckdb.hpp"`, `#include "duckdb/common/exception.hpp"`, `#include "duckdb/function/scalar_function.hpp"`.
- Use `#pragma once` as the include guard in headers.

### Types and Data Handling
- **String Handling**: Use `string_t` for database strings instead of `std::string` inside execution loops. `string_t` provides inlined short strings and zero-copy semantics. 
  - For returning strings, use `StringVector::AddString(result_vector, "my string")`.
- **Vectors & Chunks**: DuckDB operates on columnar batches. Data is passed in `DataChunk`s and `Vector`s.

### Vectorized Execution (Crucial)
Do not iterate over chunks manually using simple `for` loops if a standard DuckDB executor fits.
- Use `UnaryExecutor::Execute<INPUT_TYPE, OUTPUT_TYPE>(...)` for functions with 1 argument.
- Use `BinaryExecutor::Execute<LEFT_TYPE, RIGHT_TYPE, OUTPUT_TYPE>(...)` for functions with 2 arguments.
- DuckDB's executors automatically handle NULLs and constant vectors efficiently.

### Error Handling
Never throw standard exceptions (`std::runtime_error`) or use raw `assert()`. Use DuckDB's exception system:
- **Invalid Input**: `throw InvalidInputException("Explanation");`
- **Not Supported**: `throw NotImplementedException("Feature X is not supported");`
- **Internal Error**: `throw InternalException("State corrupted");`
- Include `#include "duckdb/common/exception.hpp"` to access these.

### Memory Management
- **No naked pointers**: Use `unique_ptr` or `shared_ptr`.
- **Arena Allocation**: During query execution, rely on DuckDB's `ArenaAllocator` or standard internal allocators attached to `ExpressionState` or `ClientContext` to avoid memory leaks and ensure thread safety.

### Naming Conventions
- **Classes/Structs**: `PascalCase` (e.g., `QuackFunction`, `QuackState`)
- **Functions/Methods**: `PascalCase` (e.g., `ExecuteQuack`, `LoadInternal`)
- **Variables/Parameters**: `snake_case` (e.g., `input_vector`, `result_data`)
- **Constants/Macros**: `UPPER_SNAKE_CASE` (e.g., `QUACK_MAX_LENGTH`)
- **Member variables**: Typically `snake_case` (or rarely `camelCase`). Check existing structs for local context.

## 6. Dependencies & External Libraries
If adding external C/C++ libraries, prefer integrating them via `vcpkg` if supported by DuckDB's CI tools, or include them as well-scoped CMake submodules.
Ensure any external includes do not leak warnings into the main build (e.g., wrap in `#pragma GCC diagnostic ignored`).
