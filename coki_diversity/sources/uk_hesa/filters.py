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

from ..generic import FileFilter, CategoryFilter

uk_hesa_women_academic_15_20 = FileFilter(source='uk_hesa',
                                          years=range(2015, 2021),
                                          reqs={
                                              'contract_levels': ['All'],
                                              'terms_of_employment': ['All'],
                                              'contract_marker': ['Academic'],
                                              'sex': ['Female'],
                                          })

uk_hesa_women_academic_09_12 = FileFilter(source='uk_hesa',
                                          years=range(2009, 2013),
                                          reqs={
                                              'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                              'gender': ['female'],
                                          })

uk_hesa_women_academic_13_14 = FileFilter(source='uk_hesa',
                                          years=range(2013, 2015),
                                          reqs={
                                              'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                              'sex': ['female'],
                                          })

uk_hesa_total_academic_15_20 = FileFilter(source='uk_hesa',
                                          years=range(2015, 2021),
                                          reqs={
                                              'contract_levels': ['All'],
                                              'terms_of_employment': ['All'],
                                              'contract_marker': ['Academic'],
                                              'total': ['Total'],
                                          })

uk_hesa_total_academic_09_12 = FileFilter(source='uk_hesa',
                                          years=range(2009, 2013),
                                          reqs={
                                              'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                              'gender': ['female', 'male'],
                                          })

uk_hesa_total_academic_13_14 = FileFilter(source='uk_hesa',
                                          years=range(2013, 2015),
                                          reqs={
                                              'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                              'sex': ['female', 'male'],
                                          })

academic_total_count = CategoryFilter(name='academic_total_count',
                                      filefilters=[uk_hesa_total_academic_15_20,
                                                   uk_hesa_total_academic_13_14,
                                                   uk_hesa_total_academic_09_12])
academic_women_count = CategoryFilter(name='academic_women_count',
                                      filefilters=[uk_hesa_women_academic_15_20,
                                                   uk_hesa_women_academic_13_14,
                                                   uk_hesa_women_academic_09_12])

if (len(academic_total_count.filefilters) == 0) or (len(academic_women_count.filefilters) == 0):
    raise NotImplementedError('Implementation of academic_total_count and academic_women_count is required')

filter_list = [academic_total_count, academic_women_count]
