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
  double avg_days;
  double avg_attendance;
  double avg_satisfaction;
} CohortAlert;

static void trim(char *s) {
  char *end;
  while (isspace((unsigned char)*s)) s++;
  if (*s == 0) return;
  end = s + strlen(s) - 1;
  while (end > s && isspace((unsigned char)*end)) end--;
  *(end + 1) = '\0';
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

static void usage(const char *name) {
  printf("Group Scholar Cohort Health Sentinel\n\n");
  printf("Usage: %s --input <file> [--json <file>] [--as-of YYYY-MM-DD] [--limit N]\n", name);
  printf("          [--alert-threshold PCT] [--min-cohort-size N]\n\n");
  printf("Options:\n");
  printf("  --input   CSV file with scholar engagement data\n");
  printf("  --json    Write JSON output to file\n");
  printf("  --as-of   Reference date for recency calculations\n");
  printf("  --limit   Limit number of risk entries shown (default 10)\n");
  printf("  --alert-threshold  High-risk share that triggers cohort alert (default 0.30)\n");
  printf("  --min-cohort-size  Minimum cohort size for alerts (default 5)\n");
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

int main(int argc, char **argv) {
  const char *input = NULL;
  const char *json_path = NULL;
  const char *as_of_str = NULL;
  int limit = 10;
  double alert_threshold = 0.30;
  int min_cohort_size = 5;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
      input = argv[++i];
    } else if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
      json_path = argv[++i];
    } else if (strcmp(argv[i], "--as-of") == 0 && i + 1 < argc) {
      as_of_str = argv[++i];
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

  if (!input) {
    usage(argv[0]);
    return 1;
  }

  if (limit < 0) limit = 0;
  if (alert_threshold < 0) alert_threshold = 0;
  if (alert_threshold > 1.0) alert_threshold = 1.0;
  if (min_cohort_size < 1) min_cohort_size = 1;

  FILE *fp = fopen(input, "r");
  if (!fp) {
    perror("Failed to open input file");
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

    if (!parse_int(fields[3], &s.touchpoints_30d)) s.valid = 0;
    if (!parse_double(fields[4], &s.attendance_rate)) s.valid = 0;
    if (!parse_double(fields[5], &s.satisfaction_score)) s.valid = 0;

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
    if (!s->valid) continue;

    struct tm touch_tm;
    if (!parse_date(s->last_touchpoint, &touch_tm)) {
      invalid_rows++;
      continue;
    }

    time_t touch_time = to_time_utc(touch_tm);
    int days_since = days_between(as_of_time, touch_time);
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
  printf("Records: %d valid, %d invalid\n", valid_count, invalid_rows + (count - valid_count));
  printf("Missing IDs: %d | Missing dates: %d\n", missing_ids, missing_dates);
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

  printf("\nCohort summary\n");
  printf("Cohort\tCount\tHigh\tMedium\tLow\tAvgTouch30\tAvgAttend\tAvgSatisfaction\tAvgDaysSince\n");
  for (int i = 0; i < cohort_count; i++) {
    CohortStats *c = &cohorts[i];
    double avg_touch = c->count ? (double)c->touchpoints_sum / c->count : 0;
    double avg_att = c->count ? c->attendance_sum / c->count : 0;
    double avg_sat = c->count ? c->satisfaction_sum / c->count : 0;
    double avg_days = c->count ? c->days_since_sum / c->count : 0;
    printf("%s\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.1f\n",
           c->name, c->count, c->high, c->medium, c->low,
           avg_touch, avg_att, avg_sat, avg_days);
  }

  CohortAlert alerts[200];
  int alert_count = 0;
  for (int i = 0; i < cohort_count; i++) {
    CohortStats *c = &cohorts[i];
    if (c->count < min_cohort_size) continue;
    double high_ratio = c->count ? (double)c->high / c->count : 0;
    if (high_ratio < alert_threshold) continue;
    double avg_att = c->count ? c->attendance_sum / c->count : 0;
    double avg_sat = c->count ? c->satisfaction_sum / c->count : 0;
    double avg_days = c->count ? c->days_since_sum / c->count : 0;
    CohortAlert alert;
    memset(&alert, 0, sizeof(CohortAlert));
    snprintf(alert.cohort, MAX_NAME, "%s", c->name);
    alert.count = c->count;
    alert.high = c->high;
    alert.medium = c->medium;
    alert.low = c->low;
    alert.high_ratio = high_ratio;
    alert.avg_days = avg_days;
    alert.avg_attendance = avg_att;
    alert.avg_satisfaction = avg_sat;
    alerts[alert_count++] = alert;
  }

  if (alert_count > 0) {
    printf("\nCohort alerts (high-risk share >= %.2f, min size %d)\n", alert_threshold, min_cohort_size);
    printf("Cohort\tHighShare\tCount\tHigh\tMedium\tLow\tAvgDays\tAvgAttend\tAvgSatisfaction\n");
    for (int i = 0; i < alert_count; i++) {
      CohortAlert *a = &alerts[i];
      printf("%s\t%.2f\t%d\t%d\t%d\t%d\t%.1f\t%.2f\t%.2f\n",
             a->cohort, a->high_ratio, a->count, a->high, a->medium, a->low,
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
              valid_count, invalid_rows + (count - valid_count));
      fprintf(jf, "  \"missing\": {\"ids\": %d, \"dates\": %d},\n", missing_ids, missing_dates);
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
      for (int i = 0; i < cohort_count; i++) {
        CohortStats *c = &cohorts[i];
        double avg_touch = c->count ? (double)c->touchpoints_sum / c->count : 0;
        double avg_att = c->count ? c->attendance_sum / c->count : 0;
        double avg_sat = c->count ? c->satisfaction_sum / c->count : 0;
        double avg_days = c->count ? c->days_since_sum / c->count : 0;
        fprintf(jf, "    {\"cohort\": \"%s\", \"count\": %d, \"high\": %d, \"medium\": %d, \"low\": %d, \"avg_touchpoints_30d\": %.2f, \"avg_attendance\": %.2f, \"avg_satisfaction\": %.2f, \"avg_days_since\": %.1f}%s\n",
                c->name, c->count, c->high, c->medium, c->low,
                avg_touch, avg_att, avg_sat, avg_days,
                (i == cohort_count - 1) ? "" : ",");
      }
      fprintf(jf, "  ],\n");
      fprintf(jf, "  \"alerts\": [\n");
      for (int i = 0; i < alert_count; i++) {
        CohortAlert *a = &alerts[i];
        fprintf(jf, "    {\"cohort\": \"%s\", \"high_share\": %.2f, \"count\": %d, \"high\": %d, \"medium\": %d, \"low\": %d, \"avg_days_since\": %.1f, \"avg_attendance\": %.2f, \"avg_satisfaction\": %.2f}%s\n",
                a->cohort, a->high_ratio, a->count, a->high, a->medium, a->low,
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

  return 0;
}
