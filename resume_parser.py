# Author: Omkar Pathak

import os
import multiprocessing as mp
import io
import spacy
import pprint
from spacy.matcher import Matcher
from . import utils


class ResumeParser(object):

    def __init__(
        self,
        resume,
        skills_file=None,
        custom_regex=None,
        base_skills=True
    ):
        nlp = spacy.load('en_core_web_sm')
        custom_nlp = spacy.load('/Users/localadmin/pyresparser/custom_model')
        self.__skills_file = skills_file
        self.__base_skills = base_skills
        self.__custom_regex = custom_regex
        self.__matcher = Matcher(nlp.vocab)
        self.__details = {
            'name': None,
            'email': None,
            'mobile_number': None,
            'skills': None,
            'university': None,
            'degree': None,
            'designation': None,
            'experience': None,
            'company_names': None,
            'no_of_pages': None,
            'total_experience': None,
        }
        self.__resume = resume
        if not isinstance(self.__resume, io.BytesIO):
            ext = os.path.splitext(self.__resume)[1].split('.')[1]
        else:
            ext = self.__resume.name.split('.')[1]
        self.__text_raw = utils.extract_text(self.__resume, '.' + ext)
        self.__text = ' '.join(self.__text_raw.split())
        self.__nlp = nlp(self.__text)
        self.__custom_nlp = custom_nlp(self.__text_raw)
        self.__noun_chunks = list(self.__nlp.noun_chunks)
        self.__get_basic_details()

    def get_extracted_data(self):
        return self.__details

    def __get_basic_details(self):
        cust_ent = utils.extract_entities_wih_custom_model(
                            self.__custom_nlp
                        )
        name = utils.extract_name(self.__nlp, matcher=self.__matcher)
        email = utils.extract_email(self.__text)
        mobile = utils.extract_mobile_number(self.__text, self.__custom_regex)
        if self.__base_skills:
            skills = utils.extract_skills(
                        self.__nlp,
                        self.__noun_chunks,
                        self.__skills_file
                    )
        else:
            skills = []
        # edu = utils.extract_education(
        #               [sent.string.strip() for sent in self.__nlp.sents]
        #       )
        entities = utils.extract_entity_sections_grad(self.__text_raw)

        uni = [item for item in entities['education'] if 'University' in item ]
        # extract name
        try:
            self.__details['name'] = cust_ent['Name'][0]
        except (IndexError, KeyError):
            self.__details['name'] = name

        # extract email
        self.__details['email'] = email

        # extract mobile number
        self.__details['mobile_number'] = mobile

        # extract skills
        self.__details['skills'] = skills

        # extract college name
        try:
            self.__details['university'] = uni
        except KeyError:
            pass

        # extract education Degree
        try:
            self.__details['degree'] = cust_ent['Degree']
        except KeyError:
            pass

        # extract designation
        try:
            self.__details['designation'] = cust_ent['Designation']
        except KeyError:
            pass


        for val in ["Achievements_0_description","Achievements_0_name","Achievements_1_description","Achievements_1_name","Achievements_2_description",
      "Achievements_2_name",      "Achievements_3_description",      "Achievements_3_name",      "Achievements_4_description",      "Achievements_4_name",      "Achievements_5_description",     "Achievements_5_name", "Achievements_6_description", "Achievements_6_name", "Degree",
      "Designation",      "Employment_0_company","Employment_0_description",      "Employment_0_employment_period_end",
      "Employment_0_employment_period_start",
      "Employment_0_position","Employment_1_company",
      "Employment_1_description", "Employment_1_employment_period_end",
      "Employment_1_employment_period_start","Employment_1_position",
      "Employment_2_company","Employment_2_description",
      "Employment_2_employment_period_end","Employment_2_employment_period_start",
      "Employment_2_position","Employment_3_company",
      "Employment_3_description","Employment_3_employment_period_end",
      "Employment_3_employment_period_start","Employment_3_position",
      "Employment_4_company","Employment_4_description",
      "Employment_4_employment_period_end",      "Employment_4_employment_period_start",
      "Employment_4_position","Employment_5_company",
      "Employment_5_description","Employment_5_employment_period_end",
      "Employment_5_employment_period_start","Employment_5_position",
      "Employment_6_company","Employment_6_description",
      "Employment_6_employment_period_end",      "Employment_6_employment_period_start",
      "Employment_6_position","Employment_7_company",
      "Employment_7_description","Employment_7_employment_period_end",
      "Employment_7_employment_period_start","Employment_7_position",
      "GraduationDate","Name",
      "Skills_0",      "Skills_1",      "Skills_10",      "Skills_11",
      "Skills_12",      "Skills_13",      "Skills_14",      "Skills_15",
      "Skills_16",      "Skills_17",      "Skills_2",      "Skills_3",
      "Skills_4",      "Skills_5",      "Skills_6",      "Skills_7",
      "Skills_8",      "Skills_9",      "Summary",      "University"]:
          # extract summary
            try:
                self.__details[val] = cust_ent[val]
            except KeyError:
                pass

        # extract company names
        try:
            self.__details['company_names'] = cust_ent['Companies worked at']
        except KeyError:
            pass

        try:
            self.__details['experience'] = entities['experience']
            try:
                exp = round(
                    utils.get_total_experience(entities['experience']) / 12,
                    2
                )
                self.__details['total_experience'] = exp
            except KeyError:
                self.__details['total_experience'] = 0
        except KeyError:
            self.__details['total_experience'] = 0
        self.__details['no_of_pages'] = utils.get_number_of_pages(
                                            self.__resume
                                        )
        return


def resume_result_wrapper(resume):
    parser = ResumeParser(resume)
    return parser.get_extracted_data()


if __name__ == '__main__':
    pool = mp.Pool(mp.cpu_count())

    resumes = []
    data = []
    for root, directories, filenames in os.walk('resumes/'):
        for filename in filenames:
            file = os.path.join(root, filename)
            resumes.append(file)

    results = [
        pool.apply_async(
            resume_result_wrapper,
            args=(x,)
        ) for x in resumes
    ]

    results = [p.get() for p in results]

    pprint.pprint(results)
