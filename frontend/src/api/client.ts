import axios from "axios";

const BASE_URL = import.meta.env.VITE_API_URL ?? "";

export const api = axios.create({ baseURL: BASE_URL });

// Attach JWT token from localStorage on every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("access_token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export const API_V1 = "/api/v1";
