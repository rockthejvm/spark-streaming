package part5twitter

sealed trait SentimentType

object SentimentType {
  // scores should be between 0 and 5
  def fromScore(score: Double): SentimentType =
    if (score < 0) NOT_UNDERSTOOD
    else if (score < 1) VERY_NEGATIVE
    else if (score < 2) NEGATIVE
    else if (score < 3) NEUTRAL
    else if (score < 4) POSITIVE
    else if (score < 5) VERY_POSITIVE
    else NOT_UNDERSTOOD
}

case object VERY_NEGATIVE extends SentimentType
case object NEGATIVE extends SentimentType
case object NEUTRAL extends SentimentType
case object POSITIVE extends SentimentType
case object VERY_POSITIVE extends SentimentType
case object NOT_UNDERSTOOD extends SentimentType
