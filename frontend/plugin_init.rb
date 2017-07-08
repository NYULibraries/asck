require "memoryleak"

MemoryLeak::Resources.define(:models_to_check, proc {
                               JSONModel::HTTP.get_json('/models_to_check')
                             }, 60)

module ApplicationHelper

  def models_to_check
    MemoryLeak::Resources.get(:models_to_check)
  end
end
