const API_URL = "http://localhost:8080";

type Node = {
	id: string;
	labels: string[];
	hotProps: {
		[key: string]: string;
	};
};

export const getNode = async (id: string) => {
	const response = await fetch(`${API_URL}/api/node?id=${id}`);
	const data = (await response.json()) as Node;
	return data;
};
