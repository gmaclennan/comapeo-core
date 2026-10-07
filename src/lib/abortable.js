/**
 * Settle with `promise`, unless `signal` aborts first, in which case reject
 * with `signal.reason`. Leaves no listener behind once settled.
 *
 * @template T
 * @param {Promise<T>} promise
 * @param {AbortSignal} [signal]
 * @returns {Promise<T>}
 */
export async function abortable(promise, signal) {
  if (!signal) return promise
  signal.throwIfAborted()
  /** @type {() => void} */
  let onAbort = () => {}
  const aborted = new Promise((_, reject) => {
    onAbort = () => reject(signal.reason)
    signal.addEventListener('abort', onAbort, { once: true })
  })
  try {
    return await Promise.race([promise, aborted])
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}
