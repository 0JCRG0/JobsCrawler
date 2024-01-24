# [[UserName]]
{{ Use the user's name found in the user's cv, if none: [[UserName]] == "Not provided"}}

**[[email]]**

{{ Use email found in user's cv, if none: [[email]] == "Not provided" }}

**[[LinkedIn]]**

{{ Use LinkedIn found in user's cv, if none: [[LinkedIn]] == "Not provided"}}

**[[GitHub]]**

{{ Use GitHub found in user's cv, if none: [[GitHub]] == leave blank}}


### [[Headline]]

{{
    1. Identify the job opening's job title and add it to [[Headline]]. For example:
        Job title: Senior manager (Americas)
        User's headline: Accomplished senior manager and sales leader focused on delivering outstanding results.
}}

## Top Skills
**[[Skill]]:** [[SkillDescription]]

{{
    1. Identify 3-5 most relevant skills requested by the job opening.
        1.1. These are usually hard skills. Hard skills are specific abilities and knowledge based on experience and training.
    2. For each [[Skill]] add a brief explanation in [[SkillDescription]] to provide context and show the depth of the user's expertise. For example:
        - **Salesforce**: 4+ years leveraging Salesforce daily. Earned Salesforce Certification in January 2017.
}}

## Work Experience
**[[Company]], [[Location]]**
**[[JobTitle]] | [[MM/YYYY]]**

[[Content]]

{{
    Don’t need to include every job that the user ever had on the resume. Stick to the most relevant jobs and demonstrate their career trajectory. 
        For example:
            If the user is a Project Manager, don’t need to mention the supermarket job the user had as a teenager.

    For each relevant job follow these instructions:
        1. Fill the following variables with their respective value found in the user's cv(
            [[Company]], [[Location]]
            [[JobTitle]] | [[MM/YYYY]]
        )
        
        2. For the [[Content]] of each relevant job, tailor the user's work experience to the job description using bullet points. Each job needs to have 3-5 bullet points that follow these rules:
            2.1 Don’t use up too much of your space detailing daily duties that aren’t relevant to the job for which the user is applying.
            2.2 Put emphasis on what would be the most important for the hiring manager.
            2.3 Add context that proves that the user actually possesses the skill(s) mentioned in the Top Skills section. For example:
                If you mention a piece of software like Excel, Photoshop, or AutoCAD, mention the types of projects it was used for.
            2.4 Demonstrate the user's increasing impact and responsibility from job to job. Show that the user is capable of taking on more and more responsibility.
}}

## Education
**[[Degree]], [[GraduationYear(YYYY)]]: _[[University/College]], [[Location]]_**

{{
    1. Fill the following variables with their respective value found in the user's cv (
        [[Degree]], [[GraduationYear(YYYY)]]: [[University/College]], [[Location]]
    )

    2. Only mention a maximum of three relevant qualifications

}}

## Other achievements
**[[Achievement]] | [[MM/YYYY]]**
[[AchievementDescription]]

{{  1. If no achievements are mentioned, omit this section.
    2. Fill the following variables with their respective value found in the user's cv(
        [[Achievement]] | [[MM/YYYY]]
    )
    3. Add into [[AchievementDescription]] a brief description of a maximum of three relevant achievements.
}}