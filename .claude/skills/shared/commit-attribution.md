# Commit Attribution

Every commit you make MUST end with a `Co-Authored-By` trailer naming **the model
you are actually running as**, not a model name copied from a skill file.

Your session's system prompt states the exact trailer to use, in the form:

    Co-Authored-By: <model name> <noreply@anthropic.com>

Use that string verbatim. Skill templates write the trailer as
`Co-Authored-By: <your session's model> <noreply@anthropic.com>`; substitute your
own model identity when you run the command. For example, a session whose system
prompt names Claude Fable 5.1 writes
`Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; a session running a
different model writes that model's name instead.

Never hardcode a model name into a skill file, and never carry one forward from an
example. Spawned agents are frequently overridden to a different model than the
orchestrator (see `.claude/skills/shared/story-task-delegation.md`), so a literal name
in a template is wrong for anyone but the one model that wrote it.

PR descriptions end with the attribution line the session's system prompt gives for
pull requests, also verbatim.
