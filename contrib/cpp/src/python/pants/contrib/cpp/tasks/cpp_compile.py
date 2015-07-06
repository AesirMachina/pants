# coding=utf-8
# Copyright 2015 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os

from pants.base.build_environment import get_buildroot
from pants.base.workunit import WorkUnit
from pants.util.dirutil import safe_mkdir

from pants.contrib.cpp.tasks.cpp_task import CppTask


class CppCompile(CppTask):
  """Compiles object files from C++ sources."""

  @classmethod
  def register_options(cls, register):
    super(CppCompile, cls).register_options(register)
    register('--cc-options',
             help='Append these options to the compiler command line.')
    register('--cc-extensions',
             default=['cc', 'cxx', 'cpp'],
             help=('The list of extensions (without the .) to consider when '
                   'determining if a file is a C++ source file.'))

  @classmethod
  def product_types(cls):
    return ['objs']

  @property
  def cache_target_dirs(self):
    return True

  def execute(self):
    """Compile all sources in a given target to object files."""

    def is_cc(source):
      _, ext = os.path.splitext(source)
      return ext[1:] in self.get_options().cc_extensions

    targets = self.context.targets(self.is_cpp)

    # Compile source files to objects.
    with self.invalidated(targets, invalidate_dependents=True) as invalidation_check:
      for vt in invalidation_check.all_vts:
        for source in vt.target.sources_relative_to_buildroot():
          if is_cc(source):
            self.context.products.get('objs').add(vt.target, vt.results_dir).append(
                self._objpath(vt, source))

      for vt in invalidation_check.invalid_vts:
        with self.context.new_workunit(name='cpp-compile', labels=[WorkUnit.MULTITOOL]):
          for source in vt.target.sources_relative_to_buildroot():
            if is_cc(source):
              # TODO: Parallelise the compilation.
              # TODO: Only recompile source files that have changed since the
              #       object file was last written. Also use the output from
              #       gcc -M to track dependencies on headers.
              self._compile(vt, source)

  def _objpath(self, vt, source):
    abs_source_root = os.path.join(get_buildroot(), vt.target.target_base)
    abs_source = os.path.join(get_buildroot(), source)
    rel_source = os.path.relpath(abs_source, abs_source_root)
    root, _ = os.path.splitext(rel_source)
    obj_name = root + '.o'

    return os.path.join(vt.results_dir, obj_name)

  def _compile(self, vt, source):
    """Compile given source to an object file."""
    obj = self._objpath(vt, source)

    abs_source = os.path.join(get_buildroot(), source)

    # TODO: include dir should include dependent work dir when headers are copied there.
    include_dirs = []
    for dep in vt.target.dependencies:
      if self.is_library(dep):
        include_dirs.extend([os.path.join(get_buildroot(), dep.target_base)])

    cmd = [self.cpp_toolchain.compiler]
    cmd.extend(['-c'])
    cmd.extend(('-I{0}'.format(i) for i in include_dirs))
    cmd.extend(['-o' + obj, abs_source])
    if self.get_options().cc_options != None:
      cmd.extend([self.get_options().cc_options])

    # TODO: submit_async_work with self.run_command, [(cmd)] as a Work object.
    with self.context.new_workunit(name='cpp-compile', labels=[WorkUnit.COMPILER]) as workunit:
      self.run_command(cmd, workunit)

    self.context.log.info('Built c++ object: {0}'.format(obj))
