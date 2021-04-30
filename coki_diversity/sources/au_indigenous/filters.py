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

academic_indigenous_08_13_hc = FileFilter(source='au_indigenous',
                                          years=range(2008, 2014),
                                          count_type='headcount',
                                          reqs={'total': ['persons']}
                                          )

academic_indigenous_14_14_hc = FileFilter(source='au_indigenous',
                                          years=range(2014, 2015),
                                          count_type='headcount',
                                          reqs={'total fte': ['persons']}
                                          )

academic_indigenous_15_20_hc = FileFilter(source='au_indigenous',
                                          years=range(2015, 2021),
                                          count_type='headcount',
                                          reqs={'total': ['persons']}
                                          )

academic_indigenous_women_14_20_hc = FileFilter(source='au_indigenous',
                                                years=range(2014, 2021),
                                                count_type='headcount',
                                                reqs={'gender': ['females']}
                                                )

academic_indigenous_women_08_13_hc = FileFilter(source='au_indigenous',
                                                years=range(2008, 2014),
                                                count_type='headcount',
                                                reqs={'total': ['females']}
                                                )

academic_indigenous_count = CategoryFilter(name='academic_indigenous_count',
                                           filefilters=[academic_indigenous_15_20_hc,
                                                        academic_indigenous_14_14_hc,
                                                        academic_indigenous_08_13_hc])
academic_indigenous_women_count = CategoryFilter(name='academic_indigenous_women_count',
                                                 filefilters=[academic_indigenous_women_14_20_hc,
                                                              academic_indigenous_women_08_13_hc])

filter_list = [academic_indigenous_count, academic_indigenous_women_count]
