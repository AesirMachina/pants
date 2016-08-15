use std::collections::HashMap;

use core::{Key, TypeId, Variants};
use selectors;
use selectors::Selector;
use tasks::Tasks;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Runnable {
  func: Key,
  args: Vec<Key>,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum State {
  Waiting(Vec<Node>),
  Complete(Complete),
  Runnable(Runnable),
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Complete {
  Noop(String),
  Return(Key),
  Throw(String),
}

pub struct StepContext<'g,'t> {
  deps: HashMap<&'g Node, Complete>,
  tasks: &'t Tasks,
}

impl<'g,'t> StepContext<'g,'t> {
  /**
   * Create Nodes for each Task that might be able to compute the given product for the
   * given subject and variants.
   *
   * (analogous to NodeBuilder.gen_nodes)
   *
   * TODO: intrinsics
   */
  fn gen_nodes(&self, subject: Key, product: TypeId, variants: Variants) -> Vec<Node> {
    self.tasks.get(&product)
      .iter()
      .map(|task| {
        Node::Task(
          Task {
            subject: subject,
            product: product,
            variants: variants,
            func: task.func(),
            clause: task.input_clause(),
          }
        )
      })
      .collect()
  }

  fn get(&self, node: Node) -> Option<&Complete> {
    self.deps.get(&node)
  }

  fn none_key(&self) -> &Key {
    self.tasks.none_key()
  }
}

/**
 * Defines executing a single step for the given context.
 */
trait Step {
  fn step(&self, context: StepContext) -> State;
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Select {
  subject: Key,
  variants: Variants,
  selector: selectors::Select,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SelectLiteral {
  subject: Key,
  variants: Variants,
  selector: selectors::SelectLiteral,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SelectVariant {
  subject: Key,
  variants: Variants,
  selector: selectors::SelectVariant,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SelectDependencies {
  subject: Key,
  variants: Variants,
  selector: selectors::SelectDependencies,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SelectProjection {
  subject: Key,
  variants: Variants,
  selector: selectors::SelectProjection,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Task {
  subject: Key,
  product: TypeId,
  variants: Variants,
  func: Key,
  clause: Vec<selectors::Selector>,
}

impl Step for Task {
  fn step(&self, context: StepContext) -> State {
    // Compute dependencies for the Node, or determine whether it is a Noop.
    let dependencies = Vec::new();
    let dep_values = Vec::new();
    for selector in self.clause {
      let dep_node = Node::create(selector, self.subject, self.variants);
      let dep_state = context.get(dep_node);
      match dep_state {
        Some(&Complete::Return(value)) =>
          dep_values.push(value),
        Some(&Complete::Noop(_)) =>
          if selector.optional() {
            dep_values.push(context.none_key().clone());
          } else {
            return State::Complete(
              Complete::Noop(format!("Was missing (at least) input for {:?}.", selector))
            );
          },
        Some(&Complete::Throw(msg)) => {
          // NB: propagate thrown exception directly.
          return State::Complete(Complete::Throw(msg));
        }
        None =>
          dependencies.push(dep_node),
      }
    }

    if !dependencies.is_empty() {
      // A clause was still waiting on dependencies.
      State::Waiting(dependencies)
    } else {
      // Ready to run!
      State::Runnable(Runnable {
        func: self.func,
        args: dep_values,
      })
    }
  }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Filesystem {
  subject: Key,
  product: TypeId,
  variants: Variants,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Node {
  Select(Select),
  SelectLiteral(SelectLiteral),
  SelectVariant(SelectVariant),
  SelectDependencies(SelectDependencies),
  SelectProjection(SelectProjection),
  Task(Task),
  Filesystem(Filesystem),
}

impl Node {
  pub fn create(selector: Selector, subject: Key, variants: Variants) -> Node {
    match selector {
      Selector::Select(s) =>
        Node::Select(Select {
          subject: subject,
          variants: variants,
          selector: s,
        }),
      Selector::SelectVariant(s) =>
        Node::SelectVariant(SelectVariant {
          subject: subject,
          variants: variants,
          selector: s,
        }),
      Selector::SelectLiteral(s) =>
        // NB: Intentionally ignores subject parameter to provide a literal subject.
        Node::SelectLiteral(SelectLiteral {
          subject: s.subject,
          variants: variants,
          selector: s,
        }),
      Selector::SelectDependencies(s) =>
        Node::SelectDependencies(SelectDependencies {
          subject: subject,
          variants: variants,
          selector: s,
        }),
      Selector::SelectProjection(s) =>
        Node::SelectProjection(SelectProjection {
          subject: subject,
          variants: variants,
          selector: s,
        }),
    }
  }

  pub fn step(&self, deps: HashMap<&Node, Complete>, tasks: &Tasks) -> State {
    let context =
      StepContext {
        deps: deps,
        tasks: tasks,
      };
    match *self {
      Node::Task(n) => n.step(context),
      n => panic!("TODO! Need to implement step for: {:?}", n),
    }
  }
}