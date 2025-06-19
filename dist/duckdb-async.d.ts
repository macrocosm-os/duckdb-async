import type TDuckDb from "duckdb";
export declare const QueryResult: typeof TDuckDb.QueryResult, OPEN_CREATE: number, OPEN_FULLMUTEX: number, OPEN_PRIVATECACHE: number, OPEN_READONLY: number, OPEN_READWRITE: number, OPEN_SHAREDCACHE: number;
export type { DuckDbError, RowData, TableData } from "duckdb";
type Callback<T> = (err: TDuckDb.DuckDbError | null, res: T) => void;
export declare class Connection {
    private conn;
    private constructor();
    /**
     * Static method to create a new Connection object. Provided because constructors can not return Promises,
     * and the DuckDb Node.JS API uses a callback in the Database constructor
     */
    static create(db: Database): Promise<Connection>;
    all(sql: string, ...args: any[]): Promise<TDuckDb.TableData>;
    arrowIPCAll(sql: string, ...args: any[]): Promise<TDuckDb.ArrowArray>;
    /**
     * Executes the sql query and invokes the callback for each row of result data.
     * Since promises can only resolve once, this method uses the same callback
     * based API of the underlying DuckDb NodeJS API
     * @param sql query to execute
     * @param args parameters for template query
     * @returns
     */
    each(sql: string, ...args: [...any, Callback<TDuckDb.RowData>] | []): void;
    /**
     * Execute one or more SQL statements, without returning results.
     * @param sql queries or statements to executes (semicolon separated)
     * @param args parameters if `sql` is a parameterized template
     * @returns `Promise<void>` that resolves when all statements have been executed.
     */
    exec(sql: string, ...args: any[]): Promise<void>;
    prepareSync(sql: string, ...args: any[]): Statement;
    prepare(sql: string, ...args: any[]): Promise<Statement>;
    runSync(sql: string, ...args: any[]): Statement;
    run(sql: string, ...args: any[]): Promise<Statement>;
    register_udf(name: string, return_type: string, fun: (...args: any[]) => any): void;
    unregister_udf(name: string): Promise<void>;
    register_bulk(name: string, return_type: string, fun: (...args: any[]) => any): void;
    stream(sql: any, ...args: any[]): TDuckDb.QueryResult;
    arrowIPCStream(sql: any, ...args: any[]): Promise<TDuckDb.IpcResultStreamIterator>;
    register_buffer(name: string, array: TDuckDb.ArrowIterable, force: boolean): Promise<void>;
    unregister_buffer(name: string): Promise<void>;
    close(): Promise<void>;
}
export declare class Database {
    private db;
    private constructor();
    /**
     * Static method to create a new Database object. Provided because constructors can not return Promises,
     * and the DuckDb Node.JS API uses a callback in the Database constructor
     */
    /**
     * Static method to create a new Database object from the specified file. Provided as a static
     * method because some initialization may happen asynchronously.
     * @param path path to database file to open, or ":memory:"
     * @returns a promise that resolves to newly created Database object
     */
    static create(path: string, accessMode?: number | Record<string, string>): Promise<Database>;
    close(): Promise<void>;
    get_ddb_internal(): TDuckDb.Database;
    connect(): Promise<Connection>;
    all(sql: string, ...args: any[]): Promise<TDuckDb.TableData>;
    arrowIPCAll(sql: string, ...args: any[]): Promise<TDuckDb.ArrowArray>;
    /**
     * Executes the sql query and invokes the callback for each row of result data.
     * Since promises can only resolve once, this method uses the same callback
     * based API of the underlying DuckDb NodeJS API
     * @param sql query to execute
     * @param args parameters for template query
     * @returns
     */
    each(sql: string, ...args: [...any, Callback<TDuckDb.RowData>] | []): void;
    /**
     * Execute one or more SQL statements, without returning results.
     * @param sql queries or statements to executes (semicolon separated)
     * @param args parameters if `sql` is a parameterized template
     * @returns `Promise<void>` that resolves when all statements have been executed.
     */
    exec(sql: string, ...args: any[]): Promise<void>;
    prepareSync(sql: string, ...args: any[]): Statement;
    prepare(sql: string, ...args: any[]): Promise<Statement>;
    runSync(sql: string, ...args: any[]): Statement;
    run(sql: string, ...args: any[]): Promise<Statement>;
    register_udf(name: string, return_type: string, fun: (...args: any[]) => any): void;
    unregister_udf(name: string): Promise<void>;
    stream(sql: any, ...args: any[]): TDuckDb.QueryResult;
    arrowIPCStream(sql: any, ...args: any[]): Promise<TDuckDb.IpcResultStreamIterator>;
    serialize(): Promise<void>;
    parallelize(): Promise<void>;
    wait(): Promise<void>;
    interrupt(): void;
    register_buffer(name: string, array: TDuckDb.ArrowIterable, force: boolean): Promise<void>;
    unregister_buffer(name: string): Promise<void>;
    registerReplacementScan(replacementScan: TDuckDb.ReplacementScanCallback): Promise<void>;
}
export declare class Statement {
    private stmt;
    /**
     * Construct an async wrapper from a statement
     */
    private constructor();
    /**
     * create a Statement object that wraps a duckdb.Statement.
     * This is intended for internal use only, and should not be called directly.
     * Use `Database.prepare()` or `Database.run()` to create Statement objects.
     */
    static create_internal(stmt: TDuckDb.Statement): Statement;
    all(...args: any[]): Promise<TDuckDb.TableData>;
    arrowIPCAll(...args: any[]): Promise<TDuckDb.ArrowArray>;
    /**
     * Executes the sql query and invokes the callback for each row of result data.
     * Since promises can only resolve once, this method uses the same callback
     * based API of the underlying DuckDb NodeJS API
     * @param args parameters for template query, followed by a NodeJS style
     *             callback function invoked for each result row.
     *
     * @returns
     */
    each(...args: [...any, Callback<TDuckDb.RowData>] | []): void;
    /**
     * Call `duckdb.Statement.run` directly without awaiting completion.
     * @param args arguments passed to duckdb.Statement.run()
     * @returns this
     */
    runSync(...args: any[]): Statement;
    run(...args: any[]): Promise<Statement>;
    finalize(): Promise<void>;
    columns(): TDuckDb.ColumnInfo[];
}
