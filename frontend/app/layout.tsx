import '../styles/globals.css'

export const metadata = {
  title: 'OneMeta Speech-to-Speech POC',
  description: 'Low-latency Speech-to-Speech Translation POC',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
