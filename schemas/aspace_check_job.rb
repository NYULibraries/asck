{
  :schema => {
    "$schema" => "http://www.archivesspace.org/archivesspace.json",
    "version" => 1,
    "type" => "object",

    "properties" => {
      "skip_validation" => {"type" => "boolean", "default" => false},
      "models" => {"type" => "array", "required" => false, "items" => {"type" => "string"}}
    }
  }
}
