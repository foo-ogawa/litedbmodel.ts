/**
 * Check if an error indicates a broken/stale connection.
 * Used by both transaction retry (DBModel) and execute-level retry (drivers).
 */
export function isConnectionError(error: Error): boolean {
  const message = error.message || '';
  const code = (error as NodeJS.ErrnoException).code || '';

  return (
    message.includes('Connection terminated unexpectedly') ||
    message.includes('Connection terminated') ||
    message.includes('Client has encountered a connection error') ||
    message.includes('read ECONNRESET') ||
    message.includes('connect ECONNREFUSED') ||
    message.includes('Connection lost') ||
    message.includes('This socket has been ended by the other party') ||
    code === 'ECONNRESET' ||
    code === 'ECONNREFUSED' ||
    code === 'EPIPE' ||
    code === 'EAI_AGAIN' ||
    code === 'PROTOCOL_CONNECTION_LOST'
  );
}
