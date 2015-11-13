package services;

/**
 * An exception reported by the {@link DataStoreManager}.
 *
 * @author Jean-Michel Busca
 *
 */
public class DataStoreException extends Exception {

  //
  // CONSTANTS
  //
  private static final long serialVersionUID = 1L;

  //
  // CONSTRUCTORS
  //
  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message
   *          the detail message, which is saved for later retrieval by the
   *          Throwable.getMessage() method
   */
  public DataStoreException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified cause.
   *
   * @param cause
   *          the cause, which is saved for later retrieval by the
   *          Throwable.getCause() method
   */
  public DataStoreException(Throwable cause) {
    super(cause);
  }

}
