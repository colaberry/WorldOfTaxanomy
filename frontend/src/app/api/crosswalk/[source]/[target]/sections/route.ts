import { NextResponse } from 'next/server'
import { getStaticSections } from '@/lib/crosswalk-data'

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ source: string; target: string }> },
) {
  const { source, target } = await params
  const data = getStaticSections(source, target)
  if (!data) {
    return NextResponse.json(
      { source_system: source, target_system: target, sections: [], total_edges: 0 },
    )
  }
  return NextResponse.json(data)
}
