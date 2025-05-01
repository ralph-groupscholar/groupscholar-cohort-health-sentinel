#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#define MAX_LINE 2048
#define MAX_NAME 64
#define MAX_DATE 16

typedef struct {
  char id[MAX_NAME];
  char cohort[MAX_NAME];
  char last_touchpoint[MAX_DATE];
  int touchpoints_30d;
  double attendance_rate;
  double satisfaction_score;
  int valid;
} Scholar;

typedef struct {
  char name[MAX_NAME];
  int count;
  int high;
  int medium;
  int low;
  double attendance_sum;
  double satisfaction_sum;
  int touchpoints_sum;
  double days_since_sum;
} CohortStats;

typedef struct {
  char id[MAX_NAME];
  char cohort[MAX_NAME];
  int risk_score;
  int days_since;
  int touchpoints_30d;
  double attendance_rate;
  double satisfaction_score;
} RiskEntry;

typedef struct {
  char cohort[MAX_NAME];
  int count;
  int high;
  int medium;
  int low;
  double high_ratio;
  double risk_index;
  double avg_days;
  double avg_attendance;
  double avg_satisfaction;
} CohortAlert;

typedef struct {
  char cohort[MAX_NAME];
  int count;
  int high;
  int medium;
  int low;
  double high_share;
  double risk_index;
  double avg_touchpoints;
  double avg_attendance;
  double avg_satisfaction;
  double avg_days;
} CohortSummary;

typedef enum {
  SORT_RISK,
  SORT_HIGH,
  SORT_NAME
} CohortSort;

static CohortSort g_cohort_sort = SORT_RISK;

static void trim(char *s) {
  char *start = s;
  while (isspace((unsigned char)*start)) start++;
  if (start != s) memmove(s, start, strlen(start) + 1);
  if (*s == '\0') return;
  char *end = s + strlen(s);
  while (end > s && isspace((unsigned char)*(end - 1))) end--;
  *end = '\0';
}

static int parse_int(const char *s, int *out) {
  char *end = NULL;
  long val = strtol(s, &end, 10);
  if (end == s || *end != '\0') return 0;
  *out = (int)val;
  return 1;
}

static int parse_double(const char *s, double *out) {
  char *end = NULL;
  double val = strtod(s, &end);
  if (end == s || *end != '\0') return 0;
  *out = val;
  return 1;
}

static int parse_date(const char *s, struct tm *out) {
  int y = 0, m = 0, d = 0;
  if (sscanf(s, "%d-%d-%d", &y, &m, &d) != 3) return 0;
  if (y < 1900 || m < 1 || m > 12 || d < 1 || d > 31) return 0;
  memset(out, 0, sizeof(struct tm));
  out->tm_year = y - 1900;
  out->tm_mon = m - 1;
  out->tm_mday = d;
  out->tm_isdst = -1;
  return 1;
}

static time_t to_time_utc(struct tm tmval) {
  return mktime(&tmval);
}

static int days_between(time_t a, time_t b) {
  double diff = difftime(a, b);
  return (int)(diff / 86400.0);
}

static int risk_score_for(int days_since, int touchpoints, double attendance, double satisfaction) {
  int score = 0;
  if (days_since > 30) score += 3;
  else if (days_since > 14) score += 2;
  else if (days_since > 7) score += 1;

  if (touchpoints == 0) score += 2;
  else if (touchpoints <= 1) score += 1;

  if (attendance < 0.6) score += 2;
  else if (attendance < 0.8) score += 1;

  if (satisfaction < 3.0) score += 2;
  else if (satisfaction < 4.0) score += 1;

  return score;
}

static const char *risk_label(int score) {
  if (score >= 6) return "high";
  if (score >= 3) return "medium";
  return "low";
}

static double cohort_risk_index(int high, int medium, int low) {
  int count = high + medium + low;
  if (count == 0) return 0.0;
  return (high * 3.0 + medium * 2.0 + low * 1.0) / (double)count;
}

static CohortSort cohort_sort_mode(const char *value, int *ok) {
  if (strcmp(value, "risk") == 0) {
    *ok = 1;
    return SORT_RISK;
  }
  if (strcmp(value, "high") == 0) {
    *ok = 1;
    return SORT_HIGH;
  }
  if (strcmp(value, "name") == 0) {
    *ok = 1;
    return SORT_NAME;
  }
  *ok = 0;
  return SORT_RISK;
}

static int matches_cohort(const char *cohort, char **filters, int filter_count) {
  if (filter_count == 0) return 1;
  for (int i = 0; i < filter_count; i++) {
    if (strcmp(cohort, filters[i]) == 0) return 1;
  }
  return 0;
}

static void usage(const char *name) {
  printf("Group Scholar Cohort Health Sentinel\n\n");
  printf("Usage: %s --input <file> [--json <file>] [--as-of YYYY-MM-DD] [--limit N]\n", name);
  printf("          [--alert-threshold PCT] [--min-cohort-size N] [--cohort NAME[,NAME]]\n");
  printf("          [--cohort-sort risk|high|name] [--cohort-limit N]\n\n");
  printf("Options:\n");
  printf("  --input   CSV file with scholar engagement data\n");
  printf("  --json    Write JSON output to file\n");
  printf("  --as-of   Reference date for recency calculations\n");
  printf("  --limit   Limit number of risk entries shown (default 10)\n");
  printf("  --cohort-sort   Sort cohort summary by risk, high, or name (default risk)\n");
  printf("  --cohort-limit  Limit number of cohorts shown in summary\n");
  printf("  --alert-threshold  High-risk share that triggers cohort alert (default 0.30)\n");
  printf("  --min-cohort-size  Minimum cohort size for alerts (default 5)\n");
  printf("  --cohort  Filter results to one or more cohorts (comma-separated)\n");
}

static int find_or_add_cohort(CohortStats *cohorts, int *count, const char *name) {
  for (int i = 0; i < *count; i++) {
    if (strcmp(cohorts[i].name, name) == 0) return i;
  }
  if (*count >= 200) return -1;
  snprintf(cohorts[*count].name, MAX_NAME, "%s", name);
  cohorts[*count].count = 0;
  cohorts[*count].high = 0;
  cohorts[*count].medium = 0;
  cohorts[*count].low = 0;
  cohorts[*count].attendance_sum = 0;
  cohorts[*count].satisfaction_sum = 0;
  cohorts[*count].touchpoints_sum = 0;
  cohorts[*count].days_since_sum = 0;
  (*count)++;
  return *count - 1;
}

static int compare_risk(const void *a, const void *b) {
  const RiskEntry *ra = (const RiskEntry *)a;
  const RiskEntry *rb = (const RiskEntry *)b;
  if (rb->risk_score != ra->risk_score) return rb->risk_score - ra->risk_score;
  if (rb->days_since != ra->days_since) return rb->days_since - ra->days_since;
  return strcmp(ra->id, rb->id);
}

static int compare_cohort_summary(const void *a, const void *b) {
  const CohortSummary *ca = (const CohortSummary *)a;
  const CohortSummary *cb = (const CohortSummary *)b;
  if (g_cohort_sort == SORT_NAME) {
    return strcmp(ca->cohort, cb->cohort);
  }
  if (g_cohort_sort == SORT_HIGH) {
    if (cb->high_share > ca->high_share) return 1;
    if (cb->high_share < ca->high_share) return -1;
    if (cb->risk_index > ca->risk_index) return 1;
    if (cb->risk_index < ca->risk_index) return -1;
    return strcmp(ca->cohort, cb->cohort);
  }
  if (cb->risk_index > ca->risk_index) return 1;
  if (cb->risk_index < ca->risk_index) return -1;
  if (cb->high_share > ca->high_share) return 1;
  if (cb->high_share < ca->high_share) return -1;
  return strcmp(ca->cohort, cb->cohort);
}

static int compare_alerts(const void *a, const void *b) {
  const CohortAlert *ca = (const CohortAlert *)a;
  const CohortAlert *cb = (const CohortAlert *)b;
  if (cb->high_ratio > ca->high_ratio) return 1;
  if (cb->high_ratio < ca->high_ratio) return -1;
  if (cb->risk_index > ca->risk_index) return 1;
  if (cb->risk_index < ca->risk_index) return -1;
  return strcmp(ca->cohort, cb->cohort);
}

int main(int argc, char **argv) {
  const char *input = NULL;
  const char *json_path = NULL;
  const char *as_of_str = NULL;
  const char *cohort_filter = NULL;
  const char *cohort_sort = "risk";
  int limit = 10;
  int cohort_limit = -1;
  double alert_threshold = 0.30;
  int min_cohort_size = 5;
  char *cohort_filter_buffer = NULL;
  char **cohort_filters = NULL;
  int cohort_filter_count = 0;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
      input = argv[++i];
    } else if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
      json_path = argv[++i];
    } else if (strcmp(argv[i], "--as-of") == 0 && i + 1 < argc) {
      as_of_str = argv[++i];
    } else if (strcmp(argv[i], "--cohort") == 0 && i + 1 < argc) {
      cohort_filter = argv[++i];
    } else if (strcmp(argv[i], "--cohort-sort") == 0 && i + 1 < argc) {
      cohort_sort = argv[++i];
    } else if (strcmp(argv[i], "--cohort-limit") == 0 && i + 1 < argc) {
      if (!parse_int(argv[++i], &cohort_limit)) {
        fprintf(stderr, "Invalid --cohort-limit value.\n");
        return 1;
      }
    } else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
      limit = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--alert-threshold") == 0 && i + 1 < argc) {
      if (!parse_double(argv[++i], &alert_threshold)) {
        fprintf(stderr, "Invalid --alert-threshold value.\n");
        return 1;
      }
    } else if (strcmp(argv[i], "--min-cohort-size") == 0 && i + 1 < argc) {
      if (!parse_int(argv[++i], &min_cohort_size)) {
        fprintf(stderr, "Invalid --min-cohort-size value.\n");
        return 1;
      }
    } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
  }

  int sort_ok = 0;
  g_cohort_sort = cohort_sort_mode(cohort_sort, &sort_ok);
  if (!sort_ok) {
    fprintf(stderr, "Invalid --cohort-sort value. Use risk, high, or name.\n");
    return 1;
  }

  if (!input) {
    usage(argv[0]);
    return 1;
  }

  if (cohort_filter) {
    cohort_filter_buffer = strdup(cohort_filter);
    if (!cohort_filter_buffer) {
      fprintf(stderr, "Failed to allocate cohort filter buffer.\n");
      return 1;
    }
    int slots = 4;
    cohort_filters = (char **)malloc(sizeof(char *) * slots);
    if (!cohort_filters) {
      fprintf(stderr, "Failed to allocate cohort filters.\n");
      free(cohort_filter_buffer);
      return 1;
    }
    char *token = strtok(cohort_filter_buffer, ",");
    while (token) {
      trim(token);
      if (*token) {
        if (cohort_filter_count >= slots) {
          slots *= 2;
          char **resized = (char **)realloc(cohort_filters, sizeof(char *) * slots);
          if (!resized) {
            fprintf(stderr, "Failed to expand cohort filters.\n");
            free(cohort_filter_buffer);
            free(cohort_filters);
            return 1;
          }
          cohort_filters = resized;
        }
        cohort_filters[cohort_filter_count++] = token;
      }
      token = strtok(NULL, ",");
    }
    if (cohort_filter_count == 0) {
      free(cohort_filters);
      cohort_filters = NULL;
    }
  }

  if (limit < 0) limit = 0;
  if (cohort_limit < -1) cohort_limit = -1;
  if (alert_threshold < 0) alert_threshold = 0;
  if (alert_threshold > 1.0) alert_threshold = 1.0;
  if (min_cohort_size < 1) min_cohort_size = 1;

  FILE *fp = fopen(input, "r");
  if (!fp) {
    perror("Failed to open input file");
    free(cohort_filter_buffer);
    free(cohort_filters);
    return 1;
  }

  struct tm as_of_tm;
  time_t as_of_time;
  if (as_of_str) {
    if (!parse_date(as_of_str, &as_of_tm)) {
      fprintf(stderr, "Invalid --as-of date. Use YYYY-MM-DD.\n");
      fclose(fp);
      return 1;
    }
    as_of_time = to_time_utc(as_of_tm);
  } else {
    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    as_of_time = to_time_utc(*local);
  }

  char line[MAX_LINE];
  int line_num = 0;
  int capacity = 128;
  int count = 0;
  Scholar *scholars = (Scholar *)malloc(sizeof(Scholar) * capacity);

  int missing_dates = 0;
  int missing_ids = 0;
  int invalid_rows = 0;
  int invalid_columns = 0;
  int invalid_numeric = 0;
  int invalid_date_format = 0;
  int future_dates = 0;

  while (fgets(line, sizeof(line), fp)) {
    line_num++;
    if (line_num == 1) continue;
    if (count >= capacity) {
      capacity *= 2;
      scholars = (Scholar *)realloc(scholars, sizeof(Scholar) * capacity);
    }

    char *token;
    char *rest = line;
    char *fields[6];
    int field_count = 0;

    while ((token = strtok_r(rest, ",", &rest)) && field_count < 6) {
      fields[field_count++] = token;
    }

    if (field_count < 6) {
      invalid_rows++;
      invalid_columns++;
      continue;
    }

    for (int i = 0; i < 6; i++) {
      trim(fields[i]);
    }

    Scholar s;
    memset(&s, 0, sizeof(Scholar));
    s.valid = 1;

    snprintf(s.id, MAX_NAME, "%s", fields[0]);
    snprintf(s.cohort, MAX_NAME, "%s", fields[1]);
    snprintf(s.last_touchpoint, MAX_DATE, "%s", fields[2]);

    if (strlen(s.id) == 0) {
      missing_ids++;
      s.valid = 0;
    }

    if (strlen(s.last_touchpoint) == 0) {
      missing_dates++;
      s.valid = 0;
    }

    int numeric_invalid = 0;
    if (!parse_int(fields[3], &s.touchpoints_30d)) {
      s.valid = 0;
      numeric_invalid = 1;
    }
    if (!parse_double(fields[4], &s.attendance_rate)) {
      s.valid = 0;
      numeric_invalid = 1;
    }
    if (!parse_double(fields[5], &s.satisfaction_score)) {
      s.valid = 0;
      numeric_invalid = 1;
    }
    if (numeric_invalid) invalid_numeric++;

    scholars[count++] = s;
  }

  fclose(fp);

  int valid_count = 0;
  int high_count = 0;
  int medium_count = 0;
  int low_count = 0;

  RiskEntry *risks = (RiskEntry *)malloc(sizeof(RiskEntry) * count);
  int risk_count = 0;

  CohortStats cohorts[200];
  int cohort_count = 0;

  for (int i = 0; i < count; i++) {
    Scholar *s = &scholars[i];
    if (!s->valid) {
      invalid_rows++;
      continue;
    }

    if (!matches_cohort(s->cohort, cohort_filters, cohort_filter_count)) {
      continue;
    }

    struct tm touch_tm;
    if (!parse_date(s->last_touchpoint, &touch_tm)) {
      invalid_rows++;
      invalid_date_format++;
      continue;
    }

    time_t touch_time = to_time_utc(touch_tm);
    int days_since = days_between(as_of_time, touch_time);
    if (days_since < 0) {
      future_dates++;
      days_since = 0;
    }
    int score = risk_score_for(days_since, s->touchpoints_30d, s->attendance_rate, s->satisfaction_score);

    const char *label = risk_label(score);
    if (strcmp(label, "high") == 0) high_count++;
    else if (strcmp(label, "medium") == 0) medium_count++;
    else low_count++;

    valid_count++;

    int cidx = find_or_add_cohort(cohorts, &cohort_count, s->cohort);
    if (cidx >= 0) {
      cohorts[cidx].count++;
      if (strcmp(label, "high") == 0) cohorts[cidx].high++;
      else if (strcmp(label, "medium") == 0) cohorts[cidx].medium++;
      else cohorts[cidx].low++;
      cohorts[cidx].attendance_sum += s->attendance_rate;
      cohorts[cidx].satisfaction_sum += s->satisfaction_score;
      cohorts[cidx].touchpoints_sum += s->touchpoints_30d;
      cohorts[cidx].days_since_sum += days_since;
    }

    RiskEntry entry;
    memset(&entry, 0, sizeof(RiskEntry));
    snprintf(entry.id, MAX_NAME, "%s", s->id);
    snprintf(entry.cohort, MAX_NAME, "%s", s->cohort);
    entry.risk_score = score;
    entry.days_since = days_since;
    entry.touchpoints_30d = s->touchpoints_30d;
    entry.attendance_rate = s->attendance_rate;
    entry.satisfaction_score = s->satisfaction_score;
    risks[risk_count++] = entry;
  }

  qsort(risks, risk_count, sizeof(RiskEntry), compare_risk);
  if (limit > risk_count) limit = risk_count;

  printf("Group Scholar Cohort Health Sentinel\n");
  printf("Reference date: %s\n", as_of_str ? as_of_str : "today");
  printf("Records: %d valid, %d invalid\n", valid_count, invalid_rows);
  printf("Missing IDs: %d | Missing dates: %d | Future dates: %d\n", missing_ids, missing_dates, future_dates);
  printf("Invalid breakdown: columns %d | numeric %d | date format %d\n",
         invalid_columns, invalid_numeric, invalid_date_format);
  printf("Risk mix: %d high | %d medium | %d low\n\n", high_count, medium_count, low_count);

  if (limit > 0) {
    printf("Top %d risk entries\n", limit);
    printf("ID\tCohort\tScore\tDays\tTouch30\tAttend\tSatisfaction\n");
    for (int i = 0; i < limit; i++) {
      RiskEntry *r = &risks[i];
      printf("%s\t%s\t%d\t%d\t%d\t%.2f\t%.2f\n",
             r->id, r->cohort, r->risk_score, r->days_since, r->touchpoints_30d,
             r->attendance_rate, r->satisfaction_score);
    }
  }

  CohortSummary *summaries = (CohortSummary *)malloc(sizeof(CohortSummary) * cohort_count);
  for (int i = 0; i < cohort_count; i++) {
    CohortStats *c = &cohorts[i];
    double avg_touch = c->count ? (double)c->touchpoints_sum / c->count : 0;
    double avg_att = c->count ? c->attendance_sum / c->count : 0;
    double avg_sat = c->count ? c->satisfaction_sum / c->count : 0;
    double avg_days = c->count ? c->days_since_sum / c->count : 0;
    double high_share = c->count ? (double)c->high / c->count : 0;
    double risk_index = cohort_risk_index(c->high, c->medium, c->low);
    CohortSummary summary;
    memset(&summary, 0, sizeof(CohortSummary));
    snprintf(summary.cohort, MAX_NAME, "%s", c->name);
    summary.count = c->count;
    summary.high = c->high;
    summary.medium = c->medium;
    summary.low = c->low;
    summary.high_share = high_share;
    summary.risk_index = risk_index;
    summary.avg_touchpoints = avg_touch;
    summary.avg_attendance = avg_att;
    summary.avg_satisfaction = avg_sat;
    summary.avg_days = avg_days;
    summaries[i] = summary;
  }

  if (cohort_count > 1) {
    qsort(summaries, cohort_count, sizeof(CohortSummary), compare_cohort_summary);
  }

  int cohort_display = cohort_count;
  if (cohort_limit >= 0 && cohort_limit < cohort_display) {
    cohort_display = cohort_limit;
  }

  printf("\nCohort summary (sorted by %s)\n", cohort_sort);
  if (cohort_display == 0) {
    printf("None\n");
  } else {
    printf("Cohort\tCount\tHigh\tMedium\tLow\tHighShare\tRiskIndex\tAvgTouch30\tAvgAttend\tAvgSatisfaction\tAvgDaysSince\n");
    for (int i = 0; i < cohort_display; i++) {
      CohortSummary *c = &summaries[i];
      printf("%s\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.1f\n",
             c->cohort, c->count, c->high, c->medium, c->low, c->high_share,
             c->risk_index, c->avg_touchpoints, c->avg_attendance, c->avg_satisfaction, c->avg_days);
    }
  }

  CohortAlert alerts[200];
  int alert_count = 0;
  for (int i = 0; i < cohort_count; i++) {
    CohortSummary *c = &summaries[i];
    if (c->count < min_cohort_size) continue;
    if (c->high_share < alert_threshold) continue;
    CohortAlert alert;
    memset(&alert, 0, sizeof(CohortAlert));
    snprintf(alert.cohort, MAX_NAME, "%s", c->cohort);
    alert.count = c->count;
    alert.high = c->high;
    alert.medium = c->medium;
    alert.low = c->low;
    alert.high_ratio = c->high_share;
    alert.risk_index = c->risk_index;
    alert.avg_days = c->avg_days;
    alert.avg_attendance = c->avg_attendance;
    alert.avg_satisfaction = c->avg_satisfaction;
    alerts[alert_count++] = alert;
  }

  if (alert_count > 0) {
    qsort(alerts, alert_count, sizeof(CohortAlert), compare_alerts);
    printf("\nCohort alerts (high-risk share >= %.2f, min size %d)\n", alert_threshold, min_cohort_size);
    printf("Cohort\tHighShare\tRiskIndex\tCount\tHigh\tMedium\tLow\tAvgDays\tAvgAttend\tAvgSatisfaction\n");
    for (int i = 0; i < alert_count; i++) {
      CohortAlert *a = &alerts[i];
      printf("%s\t%.2f\t%.2f\t%d\t%d\t%d\t%d\t%.1f\t%.2f\t%.2f\n",
             a->cohort, a->high_ratio, a->risk_index, a->count, a->high, a->medium, a->low,
             a->avg_days, a->avg_attendance, a->avg_satisfaction);
    }
  } else {
    printf("\nCohort alerts (high-risk share >= %.2f, min size %d)\n", alert_threshold, min_cohort_size);
    printf("None\n");
  }

  if (json_path) {
    FILE *jf = fopen(json_path, "w");
    if (!jf) {
      perror("Failed to write JSON output");
    } else {
      fprintf(jf, "{\n");
      fprintf(jf, "  \"reference_date\": \"%s\",\n", as_of_str ? as_of_str : "today");
      fprintf(jf, "  \"records\": {\"valid\": %d, \"invalid\": %d},\n",
              valid_count, invalid_rows);
      fprintf(jf, "  \"cohort_sort\": \"%s\",\n", cohort_sort);
      fprintf(jf, "  \"cohort_total\": %d,\n", cohort_count);
      fprintf(jf, "  \"cohort_limit\": %d,\n", cohort_display);
      if (cohort_filter_count > 0) {
        fprintf(jf, "  \"cohort_filter\": [");
        for (int i = 0; i < cohort_filter_count; i++) {
          fprintf(jf, "\"%s\"%s", cohort_filters[i], (i == cohort_filter_count - 1) ? "" : ", ");
        }
        fprintf(jf, "],\n");
      }
      fprintf(jf, "  \"missing\": {\"ids\": %d, \"dates\": %d},\n", missing_ids, missing_dates);
      fprintf(jf, "  \"invalid_breakdown\": {\"columns\": %d, \"numeric\": %d, \"date_format\": %d},\n",
              invalid_columns, invalid_numeric, invalid_date_format);
      fprintf(jf, "  \"date_anomalies\": {\"future_dates\": %d},\n", future_dates);
      fprintf(jf, "  \"risk_mix\": {\"high\": %d, \"medium\": %d, \"low\": %d},\n",
              high_count, medium_count, low_count);
      fprintf(jf, "  \"alert_threshold\": %.2f,\n", alert_threshold);
      fprintf(jf, "  \"min_cohort_size\": %d,\n", min_cohort_size);
      fprintf(jf, "  \"top_risks\": [\n");
      for (int i = 0; i < limit; i++) {
        RiskEntry *r = &risks[i];
        fprintf(jf, "    {\"id\": \"%s\", \"cohort\": \"%s\", \"score\": %d, \"days_since\": %d, \"touchpoints_30d\": %d, \"attendance_rate\": %.2f, \"satisfaction_score\": %.2f}%s\n",
                r->id, r->cohort, r->risk_score, r->days_since, r->touchpoints_30d,
                r->attendance_rate, r->satisfaction_score,
                (i == limit - 1) ? "" : ",");
      }
      fprintf(jf, "  ],\n");
      fprintf(jf, "  \"cohorts\": [\n");
      for (int i = 0; i < cohort_display; i++) {
        CohortSummary *c = &summaries[i];
        fprintf(jf, "    {\"cohort\": \"%s\", \"count\": %d, \"high\": %d, \"medium\": %d, \"low\": %d, \"high_share\": %.2f, \"risk_index\": %.2f, \"avg_touchpoints_30d\": %.2f, \"avg_attendance\": %.2f, \"avg_satisfaction\": %.2f, \"avg_days_since\": %.1f}%s\n",
                c->cohort, c->count, c->high, c->medium, c->low, c->high_share,
                c->risk_index, c->avg_touchpoints, c->avg_attendance, c->avg_satisfaction, c->avg_days,
                (i == cohort_display - 1) ? "" : ",");
      }
      fprintf(jf, "  ],\n");
      fprintf(jf, "  \"alerts\": [\n");
      for (int i = 0; i < alert_count; i++) {
        CohortAlert *a = &alerts[i];
        fprintf(jf, "    {\"cohort\": \"%s\", \"high_share\": %.2f, \"risk_index\": %.2f, \"count\": %d, \"high\": %d, \"medium\": %d, \"low\": %d, \"avg_days_since\": %.1f, \"avg_attendance\": %.2f, \"avg_satisfaction\": %.2f}%s\n",
                a->cohort, a->high_ratio, a->risk_index, a->count, a->high, a->medium, a->low,
                a->avg_days, a->avg_attendance, a->avg_satisfaction,
                (i == alert_count - 1) ? "" : ",");
      }
      fprintf(jf, "  ]\n");
      fprintf(jf, "}\n");
      fclose(jf);
    }
  }

  free(risks);
  free(scholars);
  free(summaries);
  free(cohort_filter_buffer);
  free(cohort_filters);

  return 0;
}
