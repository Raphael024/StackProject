# Stack Overflow Unanswered Questions Analysis (Trial Project)

**Project Overview**
This project is part of a trial exercise to simulate real-world analytics work at Rittman Analytics.  
The goal is to analyze **unanswered but trending topics on Stack Overflow** by building an **end-to-end data pipeline**:

- Source data from the **Stack Overflow public dataset** in BigQuery  
- Model the data into a **star schema** using dbt  
- Expose clean fact/dimension tables for analysis  
- Create a **dashboard in Google Data Studio (Looker Studio)** to tell the story

---

**Tools & Technologies**
- **BigQuery** – data warehouse, hosting the Stack Overflow dataset  
- **dbt Core** – data modeling, transformations, and testing  
- **Google Data Studio (Looker Studio)** – data visualization and storytelling  
- **GitHub** – version control and code review  

---

## Project Structure
├── models/
│ ├── Silver_Layer/ # Staging models (cleaned sources)
│ ├── Gold_Layer/
│ │ ├── Dim/ # Dimension tables (users, tags, questions, date)
│ │ ├── Facts/ # Fact tables & bridge tables
│ └── Semantic_Layer/ # Final wide report table (Major_Report)
├── snapshots/ # (Not used in this trial)
├── seeds/ # (Not used in this trial)
├── dbt_project.yml # dbt project config
└── README.md # Project documentation

---

**Data Model (Star Schema)**

**Facts:**
- `facts_questions_vw` → one row per question  
- `facts_answers_vw` → one row per answer  
- `bridge_questions_tag_vw` → resolves many-to-many between questions and tags  

**Dimensions:**
- `dim_date` → calendar attributes (year, month, quarter, etc.)  
- `dim_users` → user attributes (askers, answerers, accepted answer users)  
- `dim_questions` → question metadata (title, URL)  
- `dim_tags` → tag master data  

**Semantic Layer:**
- `Major_Report` → combines facts + dims into a wide, analysis-ready table  

---

**Setup & Running**

**Clone the repo**
```bash
git clone https://github.com/<raphael024>/<StackProject>.git
cd <StackProject>

