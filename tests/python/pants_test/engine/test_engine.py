# coding=utf-8
# Copyright 2015 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os
import unittest
from contextlib import closing, contextmanager

from pants.build_graph.address import Address
from pants.engine.engine import (ExecutionError, LocalMultiprocessEngine, LocalSerialEngine,
                                 SerializationError)
from pants.engine.nodes import Return, Throw
from pants.engine.storage import Cache, Storage
from pants.engine.subsystem.native import Native
from pants_test.engine.examples.planners import Classpath, UnpickleableResult, setup_json_scheduler
from pants_test.subsystem.subsystem_util import subsystem_instance


class EngineTest(unittest.TestCase):
  def setUp(self):
    build_root = os.path.join(os.path.dirname(__file__), 'examples', 'scheduler_inputs')
    with subsystem_instance(Native.Factory) as native_factory:
      self.scheduler = setup_json_scheduler(build_root, native_factory.create())

    self.java = Address.parse('src/java/codegen/simple')

  def request(self, goals, *addresses):
    return self.scheduler.build_request(goals=goals,
                                        subjects=addresses)

  def assert_engine(self, engine):
    result = engine.execute(self.request(['compile'], self.java))
    self.assertEqual([Return(Classpath(creator='javac'))], result.root_products.values())
    self.assertIsNone(result.error)

  @contextmanager
  def serial_engine(self):
    with closing(LocalSerialEngine(self.scheduler)) as e:
      yield e

  @contextmanager
  def multiprocessing_engine(self, pool_size=None):
    storage = Storage.create(in_memory=False)
    cache = Cache.create(storage=storage)
    with closing(LocalMultiprocessEngine(self.scheduler, storage, cache,
                                         pool_size=pool_size, debug=True)) as e:
      yield e

  def test_serial_engine_simple(self):
    with self.serial_engine() as engine:
      self.assert_engine(engine)

  def test_multiprocess_engine_multi(self):
    with self.multiprocessing_engine() as engine:
      self.assert_engine(engine)

  def test_multiprocess_engine_single(self):
    with self.multiprocessing_engine(pool_size=1) as engine:
      self.assert_engine(engine)

  def test_multiprocess_unpickleable(self):
    build_request = self.request(['unpickleable'], self.java)

    with self.multiprocessing_engine() as engine:
      result = engine.execute(build_request)
      self.assertIsNone(result.error)

      self.assertEquals(1, len(result.root_products))
      root_product = result.root_products.values()[0]
      self.assertEquals(Throw, type(root_product))
      self.assertEquals(SerializationError, type(root_product.exc))

  def test_rerun_with_cache(self):
    with self.multiprocessing_engine() as engine:
      # Run once and save stats to prepare for another run.
      self.assert_engine(engine)
      cache_stats = engine.cache_stats()
      hits, misses = cache_stats.misses, cache_stats.misses

      # First run there will only be duplicate executions cached (ie, the same Runnable
      # is triggered by multiple Nodes).
      self.assertTrue(cache_stats.hits > 0)
      self.assertTrue(cache_stats.misses > 0)

      self.scheduler.product_graph.invalidate()
      self.assert_engine(engine)

      # Second run hits have increaed, and there are no more misses.
      self.assertEquals(misses, cache_stats.misses)
      self.assertTrue(hits < cache_stats.hits)

  def test_product_request_throw(self):
    with self.serial_engine() as engine:
      with self.assertRaises(ExecutionError) as e:
        for _ in engine.product_request(UnpickleableResult, [self.java]):
          pass

    exc_str = str(e.exception)
    self.assertIn('Computing UnpickleableResult', exc_str)
    self.assertRegexpMatches(exc_str, 'Throw.*SerializationError')
    self.assertIn('Failed to pickle', exc_str)

  def test_product_request_return(self):
    with self.serial_engine() as engine:
      count = 0
      for computed_product in engine.product_request(Classpath, [self.java]):
        self.assertIsInstance(computed_product, Classpath)
        count += 1
      self.assertGreater(count, 0)
