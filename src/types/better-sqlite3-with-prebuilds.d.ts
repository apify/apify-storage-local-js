/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/ban-types, no-shadow */

// Licensed under MIT by DefinitelyTyped - Read more at https://github.com/DefinitelyTyped/DefinitelyTyped/blob/master/LICENSE

declare module 'better-sqlite3-with-prebuilds' {
    import Integer from 'integer';

    type VariableArgFunction = (...params: any[]) => any;
    type ArgumentTypes<F extends VariableArgFunction> = F extends (...args: infer A) => any
      ? A
      : never;

    namespace BetterSqlite3 {
        interface Statement<BindParameters extends any[]> {
            database: Database;
            source: string;
            reader: boolean;

            run(...params: BindParameters): Database.RunResult;
            get(...params: BindParameters): any;
            all(...params: BindParameters): any[];
            iterate(...params: BindParameters): IterableIterator<any>;
            pluck(toggleState?: boolean): this;
            expand(toggleState?: boolean): this;
            raw(toggleState?: boolean): this;
            bind(...params: BindParameters): this;
            columns(): ColumnDefinition[];
            safeIntegers(toggleState?: boolean): this;
        }

        interface ColumnDefinition {
            name: string;
            column: string | null;
            table: string | null;
            database: string | null;
            type: string | null;
        }

        interface Transaction<F extends VariableArgFunction> {
            (...params: ArgumentTypes<F>): ReturnType<F>;
            default(...params: ArgumentTypes<F>): ReturnType<F>;
            deferred(...params: ArgumentTypes<F>): ReturnType<F>;
            immediate(...params: ArgumentTypes<F>): ReturnType<F>;
            exclusive(...params: ArgumentTypes<F>): ReturnType<F>;
        }

        interface Database {
            memory: boolean;
            readonly: boolean;
            name: string;
            open: boolean;
            inTransaction: boolean;

            prepare<BindParameters extends any[] | {} = any[]>(source: string): BindParameters extends any[]
                ? Statement<BindParameters>
                : Statement<[BindParameters]>;
            transaction<F extends VariableArgFunction>(fn: F): Transaction<F>;
            exec(source: string): this;
            pragma(source: string, options?: Database.PragmaOptions): any;
            checkpoint(databaseName?: string): this;
            function(name: string, cb: (...params: any[]) => any): this;
            function(name: string, options: Database.RegistrationOptions, cb: (...params: any[]) => any): this;
            aggregate(name: string, options: Database.AggregateOptions): this;
            loadExtension(path: string): this;
            close(): this;
            defaultSafeIntegers(toggleState?: boolean): this;
            backup(destinationFile: string, options?: Database.BackupOptions): Promise<Database.BackupMetadata>;
        }

        interface DatabaseConstructor {
            new(filename: string, options?: Database.Options): Database;
            (filename: string, options?: Database.Options): Database;
            prototype: Database;

            Integer: typeof Integer;
            SqliteError: typeof SqliteError;
        }
    }

    class SqliteError implements Error {
        name: string;

        message: string;

        code: string;

        constructor(message: string, code: string);
    }

    namespace Database {
        interface RunResult {
            changes: number;
            lastInsertRowid: Integer.IntLike;
        }

        interface Options {
            memory?: boolean;
            readonly?: boolean;
            fileMustExist?: boolean;
            timeout?: number;
            verbose?: (message?: any, ...additionalArgs: any[]) => void;
        }

        interface PragmaOptions {
            simple?: boolean;
        }

        interface RegistrationOptions {
            varargs?: boolean;
            deterministic?: boolean;
            safeIntegers?: boolean;
        }

        interface AggregateOptions extends RegistrationOptions {
            start?: any;
            step: (total: any, next: any) => any;
            inverse?: (total: any, dropped: any) => any;
            result?: (total: any) => any;
        }

        interface BackupMetadata {
            totalPages: number;
            remainingPages: number;
        }
        interface BackupOptions {
            progress: (info: BackupMetadata) => number;
        }

        type Integer = typeof Integer;
        type SqliteError = typeof SqliteError;
        type Statement<BindParameters extends any[] | {} = any[]> = BindParameters extends any[]
          ? BetterSqlite3.Statement<BindParameters>
          : BetterSqlite3.Statement<[BindParameters]>;
        type ColumnDefinition = BetterSqlite3.ColumnDefinition;
        type Transaction = BetterSqlite3.Transaction<VariableArgFunction>;
        type Database = BetterSqlite3.Database;
    }

    const Database: BetterSqlite3.DatabaseConstructor;

    export = Database;
}
