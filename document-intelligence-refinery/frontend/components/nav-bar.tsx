"use client";

import Image from "next/image";

export function NavBar() {
  return (
    <header className="sticky top-0 z-10 border-b border-slate-200/80 bg-white/95 shadow-sm backdrop-blur-sm">
      <div className="mx-auto flex max-w-[1520px] items-center justify-between px-4 py-3.5 sm:px-6 lg:px-8">
        <div className="flex items-baseline gap-4">
          <span className="font-display text-xl font-bold tracking-tight text-slate-900 sm:text-2xl">
            DocForge
          </span>
          
        </div>
        <div className="flex shrink-0">
          <Image
            src="/images/me.jpg"
            alt="Profile"
            width={40}
            height={40}
            className="rounded-full object-cover"
          />
        </div>
      </div>
    </header>
  );
}
