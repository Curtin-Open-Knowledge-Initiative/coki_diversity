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

sa_hemis_academic_women_10_19 = FileFilter(source='sa_hemis',
                                           years=range(2010, 2020),
                                           reqs={'personnel_category': ['   1.1 INSTRUCTION/RESEARCH PROFESSIONAL'],
                                                 'GENDER': ['FEMALE'],
                                                 }
                                           )

sa_hemis_academic_women_01_09 = FileFilter(source='sa_hemis',
                                           years=range(2001, 2010),
                                           reqs={'personnel_category': ['   1.1 Instruction/Research Professional'],
                                                 'GENDER': ['FEMALE'],
                                                 }
                                           )

sa_hemis_academic_total_10_19 = FileFilter(source='sa_hemis',
                                           years=range(2010, 2020),
                                           reqs={'personnel_category': ['   1.1 INSTRUCTION/RESEARCH PROFESSIONAL'],
                                                 'RACE': ['TOTAL'],
                                                 }
                                           )

sa_hemis_academic_total_01_09 = FileFilter(source='sa_hemis',
                                           years=range(2001, 2010),
                                           reqs={'personnel_category': ['   1.1 Instruction/Research Professional'],
                                                 'RACE': ['TOTAL'],
                                                 }
                                           )

academic_total_count = CategoryFilter(name='academic_total_count',
                                      filefilters=[sa_hemis_academic_total_01_09, sa_hemis_academic_total_10_19])
academic_women_count = CategoryFilter(name='academic_women_count',
                                      filefilters=[sa_hemis_academic_women_01_09, sa_hemis_academic_women_10_19])

if (len(academic_total_count.filefilters) == 0) or (len(academic_women_count.filefilters) == 0):
    raise NotImplementedError('Implementation of academic_total_count and academic_women_count is required')

filter_list = [academic_total_count, academic_women_count]
