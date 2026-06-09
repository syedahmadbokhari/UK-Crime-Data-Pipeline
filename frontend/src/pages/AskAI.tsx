import { useState } from "react";
import { useAskQuestion } from "../api/queries";
import LoadingSpinner from "../components/LoadingSpinner";

const EXAMPLE_QUESTIONS = [
  "What was the most common crime in West Yorkshire?",
  "How many burglaries were recorded in 2026-02?",
  "What percentage of crimes are under investigation?",
];

export default function AskAI() {
  const [question, setQuestion] = useState("");
  const ask = useAskQuestion();

  return (
    <div>
      <h1 className="text-2xl font-bold mb-2">Ask AI</h1>
      <p className="text-sm text-gray-600 mb-6">Ask a natural language question about crime data. The AI answers using only retrieved database records.</p>

      <div className="bg-white rounded-lg border p-6 mb-6">
        <textarea
          className="w-full border rounded px-3 py-2 text-sm mb-3 h-20 resize-none"
          placeholder="e.g. What was the most common crime in West Yorkshire?"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />
        <div className="flex gap-2 items-center">
          <button onClick={() => ask.mutate(question)} disabled={ask.isPending || !question.trim()} className="bg-blue-600 text-white px-4 py-2 rounded text-sm hover:bg-blue-700 disabled:opacity-50">
            Ask
          </button>
          <span className="text-xs text-gray-400">Try: </span>
          {EXAMPLE_QUESTIONS.map((q) => (
            <button key={q} onClick={() => setQuestion(q)} className="text-xs text-blue-500 hover:underline truncate max-w-[160px]">{q}</button>
          ))}
        </div>
      </div>

      {ask.isPending && <LoadingSpinner label="Querying AI..." />}

      {ask.data && (
        <div className="bg-white rounded-lg border p-6">
          <p className="text-sm leading-relaxed mb-4">{ask.data.answer}</p>
          <div className="flex items-center gap-4 text-xs text-gray-500 border-t pt-3">
            <span>Records analysed: {ask.data.records_analysed.toLocaleString()}</span>
            <span>Confidence: {(ask.data.confidence * 100).toFixed(0)}%</span>
          </div>
          {ask.data.sources.length > 0 && (
            <div className="mt-3">
              <p className="text-xs font-medium text-gray-500 mb-1">Sources</p>
              {ask.data.sources.slice(0, 3).map((s) => (
                <p key={s.id} className="text-xs text-gray-400">{s.month} — {s.force} — {s.crime_type} {s.lsoa_name ? `(${s.lsoa_name})` : ""}</p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
