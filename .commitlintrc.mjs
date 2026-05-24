import process from "node:process";

const FORBIDDEN_SUBJECT_PREFIXES = [
  "mediapm:",
  "conductor:",
  "cas:",
  "echo:",
  "fs:",
  "import:",
  "export:",
  "archive:",
];

export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "scope-empty": [2, "never"],
    "subject-no-crate-prefix": [2, "always"],
  },
  plugins: [
    {
      rules: {
        "subject-no-crate-prefix": (parsed) => {
          const subject = (parsed.subject ?? "").trimStart().toLowerCase();
          const forbidden = FORBIDDEN_SUBJECT_PREFIXES.find((prefix) =>
            subject.startsWith(prefix),
          );

          return [
            forbidden === undefined,
            `commit subject must not start with crate/tool prefix '${forbidden ?? "<prefix>:"}'; use Conventional Commit scope instead (type(scope): subject)`,
          ];
        },
      },
    },
  ],
  ignores: [
    () =>
      Boolean(
        process.env.GITHUB_DEPENDABOT_CRED_TOKEN ||
        process.env.GITHUB_DEPENDABOT_JOB_TOKEN,
      ),
    (message) => message.includes("Signed-off-by: dependabot[bot]"),
  ],
};
