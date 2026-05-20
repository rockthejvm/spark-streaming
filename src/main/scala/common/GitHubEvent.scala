package common

import java.sql.Timestamp

case class GitHubEvent(
  id: String,
  eventType: String,
  actorLogin: String,
  repoName: String,
  createdAt: Timestamp
)

case class UserActivitySummary(
  actorLogin: String,
  totalEvents: Long,
  eventsByType: Map[String, Long],
  summaryType: String
)

case class GitHubAlert(
  actorLogin: String,
  alertType: String,
  message: String,
  detectedAt: Timestamp
)
