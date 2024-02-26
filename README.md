A data engineering project for my personal use for working with the undocumented ROH REST API.

It is deployed to Google Cloud as a Cloud Function, and a Cloud Run service via Docker container. Cloud Scheduler service is set up to call the Cloud Run several times a day to check for updated data from the API and store it in a Google Cloud Storage bucket. It also updates the **Upcoming events** plot below.

In order to maintain a low profile, the documentation for this repository is limited. The code is not intended for public use.

The GCP-hosted Dash app is publicly available here: **https://artnosv.com/roh-events/**

---

# Upcoming events
<picture>
<source media="(prefers-color-scheme: dark)" srcset="https://storage.googleapis.com/vitaminb16-public/output/images/ROH_events_dark.png?">
<source media="(prefers-color-scheme: light)" srcset="https://storage.googleapis.com/vitaminb16-public/output/images/ROH_events.png?">
<img src="https://storage.googleapis.com/vitaminb16-public/output/images/ROH_events.png?" width="1000"/>
</picture>

---

# Seat availability
<!-- <img src="output/ROH_hall.png" width="1000"/> -->
<picture>
<source media="(prefers-color-scheme: dark)" srcset="output/images/ROH_hall_dark.png">
<source media="(prefers-color-scheme: light)" srcset="output/images/ROH_hall.png">
<img src="output/images/ROH_hall.png" width="1000"/>
</picture>


<!-- 
TODO:
- [ ] Add Firestore utils class to `cloud` module
- [ ] Move titles_colour.json from Storage into Firestore
 -->
