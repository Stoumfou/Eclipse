package model;

import java.util.Date;

import services.DataStoreManager;

/**
 * The description of an account operation (i.e. deposit or withdrawal), as used
 * in {@link DataStoreManager}.
 * <p>
 * <b>Note: DO NOT alter this class.</b>
 *
 * @author Jean-Michel Busca
 *
 */
public class Operation {

  //
  // INSTANCE FIELDS
  //
  private final int number; // the account's number
  private final double amount; // the amount deposited/withdrawn
  private final Date date; // the date/time of operation

  //
  // CONSTRUCTOR
  //
  /**
   * Creates an new operation with the specified information.
   *
   * @param number
   *          the number of the account
   * @param amount
   *          the amount deposited (>= 0) or withdrawn (< 0)
   * @param date
   *          the date of the operation
   */
  public Operation(int number, double amount, Date date) {
    this.number = number;
    this.amount = amount;
    this.date = date;
  }

  @Override
  public String toString() {
    return "Operation [number=" + number + ", amount=" + amount + ", date="
            + date + "]";
  }

  //
  // ACCESSORS
  //
  public int getNumber() {
    return number;
  }

  public double getAmount() {
    return amount;
  }

  public Date getDate() {
    return date;
  }

  //
  // IDENTITY
  //
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(amount);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + number;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Operation other = (Operation) obj;
    if (Double.doubleToLongBits(amount) != Double
            .doubleToLongBits(other.amount))
      return false;
    if (date == null) {
      if (other.date != null)
        return false;
    } else if (!date.equals(other.date))
      return false;
    if (number != other.number)
      return false;
    return true;
  }

}
