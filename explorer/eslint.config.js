import { globalIgnores } from "eslint/config";
import reactDom from "eslint-plugin-react-dom";
import reactX from "eslint-plugin-react-x";
import tseslint from "typescript-eslint";

export default tseslint.config([
	globalIgnores(["dist"]),
	{
		files: ["**/*.{ts,tsx}"],
		extends: [
			...tseslint.configs.recommendedTypeChecked,
			...tseslint.configs.stylisticTypeChecked,
			reactX.configs["recommended-typescript"],
			reactDom.configs.recommended,
		],
		languageOptions: {
			parserOptions: {
				project: ["./tsconfig.node.json", "./tsconfig.app.json"],
				tsconfigRootDir: import.meta.dirname,
			},
		},
	},
]);
