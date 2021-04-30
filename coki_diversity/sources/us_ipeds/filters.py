# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Cameron Neylon

"""Filters for Diversity Data Normalisation

Some documentation goes here describing the outline process

Any source must provide at minimum a filter that provides a total academic_total_count (which may actually be FTE)
and an academic_women_count (similarly). If this is not feasible then changes will need to be made in the source
__init__ file to avoid errors and this should be noted in the main documentation as an exception.

"""

from ..generic import FileFilter, CategoryFilter

# Academic Headcount
us_ipeds_academic_total_15_19 = FileFilter(source='us_ipeds',
                                           years=range(2015, 2020),
                                           reqs={'occupation_filled': ['instruction', 'research', 'public service'],
                                                 'gender': ['total'],
                                                 'ethnicity': ['total']
                                                 }
                                           )

us_ipeds_academic_total_11_14 = FileFilter(source='us_ipeds',
                                           years=range(2011, 2015),
                                           reqs={'occupation_and_status': ['instructional staff', 'service'],
                                                 'gender_and_ethnicity': ['grand_total'],
                                                 }
                                           )

us_ipeds_academic_total_08_10 = FileFilter(source='us_ipeds',
                                           years=range(2008, 2011),
                                           reqs={'occupation_and_status': [
                                               'full time and part time, instruction/research/public service'],
                                               'gender_and_ethnicity': ['grand_total'],
                                           }
                                           )

us_ipeds_academic_total_01_07 = FileFilter(source='us_ipeds',
                                           years=range(2001, 2008),
                                           reqs={'occupation_and_status': [
                                               'full time and part time, faculty (instruction/research/public service)'],
                                               'gender_and_ethnicity': ['grand_total'],
                                           }
                                           )

# Academic Women Headcount
us_ipeds_academic_women_15_19 = FileFilter(source='us_ipeds',
                                           years=range(2015, 2020),
                                           reqs={'occupation_filled': ['instruction', 'research', 'public service'],
                                                 'gender': ['women'],
                                                 'ethnicity': ['total']
                                                 }
                                           )

us_ipeds_academic_women_11_14 = FileFilter(source='us_ipeds',
                                           years=range(2011, 2015),
                                           reqs={'occupation_and_status': ['instructional staff', 'service'],
                                                 'gender_and_ethnicity': ['grand_total_women'],
                                                 }
                                           )

us_ipeds_academic_women_08_10 = FileFilter(source='us_ipeds',
                                           years=range(2008, 2011),
                                           reqs={'occupation_and_status': [
                                               'full time and part time, instruction/research/public service'],
                                               'gender_and_ethnicity': ['grand_total_women'],
                                           }
                                           )

us_ipeds_academic_women_01_07 = FileFilter(source='us_ipeds',
                                           years=range(2001, 2008),
                                           reqs={'occupation_and_status': [
                                               'full time and part time, faculty (instruction/research/public service)'],
                                               'gender_and_ethnicity': ['grand_total_women'],
                                           }
                                           )

# Academic Indigenous
us_ipeds_academic_indigenous_15_19 = FileFilter(source='us_ipeds',
                                                years=range(2015, 2020),
                                                reqs={
                                                    'occupation_filled': ['instruction', 'research', 'public service'],
                                                    'gender': ['total'],
                                                    'ethnicity': ['american_indian_or_alaska_native',
                                                                  'native_hawaiian_or_other_pacific_islander']
                                                    }
                                                )

us_ipeds_academic_indigenous_11_14 = FileFilter(source='us_ipeds',
                                                years=range(2011, 2015),
                                                reqs={'occupation_and_status': ['instructional staff', 'service'],
                                                      'gender_and_ethnicity': [
                                                          'american_indian_or_alaska_native_total',
                                                          'native_hawaiian_or_other_pacific_islander_total']
                                                      }
                                                )

us_ipeds_academic_indigenous_10_10 = FileFilter(source='us_ipeds',
                                                years=range(2010, 2011),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'american_indian_or_alaska_native_total',
                                                        'native_hawaiian_or_other_pacific_islander_total'
                                                    ]
                                                }
                                                )

us_ipeds_academic_indigenous_08_09 = FileFilter(source='us_ipeds',
                                                years=range(2008, 2010),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'american_indian_or_alaska_native_total_-_new',
                                                        'native_hawaiian_or_other_pacific_islander_total_-_new'
                                                    ]
                                                }
                                                )

us_ipeds_academic_indigenous_01_07 = FileFilter(source='us_ipeds',
                                                years=range(2001, 2008),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, '
                                                    'faculty (instruction/research/public service)'],
                                                    'gender_and_ethnicity': ['american_indian_or_alaska_native_total'],
                                                }
                                                )

# Academic Indigenous Women
us_ipeds_academic_indigenous_women_15_19 = FileFilter(source='us_ipeds',
                                                years=range(2015, 2020),
                                                reqs={
                                                    'occupation_filled': ['instruction', 'research', 'public service'],
                                                    'gender': ['women'],
                                                    'ethnicity': ['american_indian_or_alaska_native',
                                                                  'native_hawaiian_or_other_pacific_islander']
                                                    }
                                                )

us_ipeds_academic_indigenous_women_11_14 = FileFilter(source='us_ipeds',
                                                years=range(2011, 2015),
                                                reqs={'occupation_and_status': ['instructional staff', 'service'],
                                                      'gender_and_ethnicity': [
                                                          'american_indian_or_alaska_native_women',
                                                          'native_hawaiian_or_other_pacific_islander_women'
                                                      ]
                                                      }
                                                )

us_ipeds_academic_indigenous_women_10_10 = FileFilter(source='us_ipeds',
                                                years=range(2010, 2011),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'american_indian_or_alaska_native_women',
                                                        'native_hawaiian_or_other_pacific_islander_women'
                                                    ]
                                                }
                                                )

us_ipeds_academic_indigenous_women_08_09 = FileFilter(source='us_ipeds',
                                                years=range(2008, 2010),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'american_indian_or_alaska_native_women_-_new',
                                                        'native_hawaiian_or_other_pacific_islander_women_-_new'
                                                    ]
                                                }
                                                )

us_ipeds_academic_indigenous_women_01_07 = FileFilter(source='us_ipeds',
                                                years=range(2001, 2008),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, '
                                                    'faculty (instruction/research/public service)'],
                                                    'gender_and_ethnicity': ['american_indian_or_alaska_native_women'],
                                                }
                                                )

# Academic White

us_ipeds_academic_white_15_19 = FileFilter(source='us_ipeds',
                                                years=range(2015, 2020),
                                                reqs={
                                                    'occupation_filled': ['instruction', 'research', 'public service'],
                                                    'gender': ['total'],
                                                    'ethnicity': ['white']
                                                    }
                                                )

us_ipeds_academic_white_11_14 = FileFilter(source='us_ipeds',
                                                years=range(2011, 2015),
                                                reqs={'occupation_and_status': ['instructional staff', 'service'],
                                                      'gender_and_ethnicity': [
                                                          'white_total']
                                                      }
                                                )

us_ipeds_academic_white_10_10 = FileFilter(source='us_ipeds',
                                                years=range(2010, 2011),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'white_total'
                                                    ]
                                                }
                                                )

us_ipeds_academic_white_08_09 = FileFilter(source='us_ipeds',
                                                years=range(2008, 2010),
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, instruction/research/public service'],
                                                    'gender_and_ethnicity': [
                                                        'white_total_-_new'
                                                    ]
                                                }
                                                )

us_ipeds_academic_white_03_03 = FileFilter(source='us_ipeds',
                                                years=[2003],
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, '
                                                    'faculty (instruction/research/public service)'],
                                                    'gender_and_ethnicity': ['white,_non-hispanic_total'],
                                                }
                                                )

us_ipeds_academic_white_01_07 = FileFilter(source='us_ipeds',
                                                years=[2001, 2002, 2004, 2005, 2006, 2007],
                                                reqs={'occupation_and_status': [
                                                    'full time and part time, '
                                                    'faculty (instruction/research/public service)'],
                                                    'gender_and_ethnicity': ['white_non-hispanic_total'],
                                                }
                                                )

academic_total_count = CategoryFilter(name='academic_total_count',
                                      filefilters=[us_ipeds_academic_total_15_19,
                                                   us_ipeds_academic_total_11_14,
                                                   us_ipeds_academic_total_08_10,
                                                   us_ipeds_academic_total_01_07])
academic_women_count = CategoryFilter(name='academic_women_count',
                                      filefilters=[us_ipeds_academic_women_15_19,
                                                   us_ipeds_academic_women_11_14,
                                                   us_ipeds_academic_women_08_10,
                                                   us_ipeds_academic_women_01_07])

academic_indigenous_count = CategoryFilter(name='academic_indigenous_count',
                                           filefilters=[us_ipeds_academic_indigenous_15_19,
                                                        us_ipeds_academic_indigenous_11_14,
                                                        us_ipeds_academic_indigenous_10_10,
                                                        us_ipeds_academic_indigenous_08_09,
                                                        us_ipeds_academic_indigenous_01_07])

academic_indigenous_women_count = CategoryFilter(name='academic_indigenous_women_count',
                                           filefilters=[us_ipeds_academic_indigenous_women_15_19,
                                                        us_ipeds_academic_indigenous_women_11_14,
                                                        us_ipeds_academic_indigenous_women_10_10,
                                                        us_ipeds_academic_indigenous_women_08_09,
                                                        us_ipeds_academic_indigenous_women_01_07])

academic_white_count = CategoryFilter(name='academic_white_count',
                                      filefilters=[us_ipeds_academic_white_15_19,
                                                   us_ipeds_academic_white_11_14,
                                                   us_ipeds_academic_white_10_10,
                                                   us_ipeds_academic_white_08_09,
                                                   us_ipeds_academic_white_01_07,
                                                   us_ipeds_academic_white_03_03])

if (len(academic_total_count.filefilters) == 0) or (len(academic_women_count.filefilters) == 0):
    raise NotImplementedError('Implementation of academic_total_count and academic_women_count is required')

filter_list = [academic_total_count, academic_women_count,
               academic_indigenous_count, academic_indigenous_women_count,
               academic_white_count]
